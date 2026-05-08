// ChapterPromptContextTests.swift
// playhead-au2v.1.16: Light regression coverage for the bead-16
// chapter-context infrastructure. Covers the formatter contract,
// selector mode-gate / lookup behavior, and diagnostic event
// round-trip. Bead 17 owns deeper FM-prompt path coverage.

import Foundation
import Testing
@testable import Playhead

@Suite("ChapterPromptContext formatter")
struct ChapterPromptContextFormatterTests {

    @Test("renders the full form when budget allows")
    func rendersFullFormWhenBudgetAllows() {
        let ctx = ChapterPromptContext(
            chapterIndex: 4,
            totalChapters: 7,
            dispositionToken: "adBreak",
            previousDispositionToken: "content",
            topicDescriptor: "Squarespace promo"
        )
        let formatted = ctx.format()
        #expect(formatted != nil)
        #expect(formatted?.contains("Chapter context: 4/7 adBreak.") == true)
        #expect(formatted?.contains("Prev: content.") == true)
        #expect(formatted?.contains("Topic: Squarespace promo.") == true)
    }

    @Test("omits Prev clause for the first chapter")
    func omitsPrevClauseForFirstChapter() throws {
        let ctx = ChapterPromptContext(
            chapterIndex: 1,
            totalChapters: 5,
            dispositionToken: "content",
            previousDispositionToken: nil,
            topicDescriptor: nil
        )
        let formatted = try #require(ctx.format())
        #expect(formatted == "Chapter context: 1/5 content.")
    }

    @Test("omits Topic clause when descriptor is nil or empty")
    func omitsTopicClauseWhenAbsent() throws {
        let ctx = ChapterPromptContext(
            chapterIndex: 2,
            totalChapters: 3,
            dispositionToken: "content",
            previousDispositionToken: "adBreak",
            topicDescriptor: ""
        )
        let formatted = try #require(ctx.format())
        #expect(!formatted.contains("Topic:"))
    }

    @Test("truncates long topic to fit budget")
    func truncatesLongTopicToFitBudget() throws {
        // A descriptor wide enough to push past the default budget under
        // the conservative `ceil(chars / 3)` estimator. The formatter
        // should halve / drop trailing words until it fits.
        let longDescriptor = String(repeating: "alpha beta ", count: 30)
        let ctx = ChapterPromptContext(
            chapterIndex: 1,
            totalChapters: 2,
            dispositionToken: "content",
            previousDispositionToken: "content",
            topicDescriptor: longDescriptor
        )
        let formatted = try #require(ctx.format())
        #expect(ChapterPromptContext.estimateTokens(of: formatted) <= ChapterPromptContext.defaultMaxTokens)
        #expect(formatted.contains("Chapter context: 1/2 content."))
    }

    @Test("returns nil when even baseline form does not fit")
    func returnsNilWhenBaselineDoesNotFit() {
        let ctx = ChapterPromptContext(
            chapterIndex: 1,
            totalChapters: 2,
            dispositionToken: "content",
            previousDispositionToken: nil,
            topicDescriptor: nil
        )
        // A budget of zero tokens cannot fit any form.
        #expect(ctx.format(maxTokens: 0) == nil)
    }
}

@Suite("ChapterPromptContextSelector")
struct ChapterPromptContextSelectorTests {

    private func chapter(
        start: Double,
        end: Double?,
        title: String?,
        disposition: ChapterDisposition,
        source: ChapterSource = .inferred,
        quality: Float = 0.8
    ) -> ChapterEvidence {
        ChapterEvidence(
            startTime: start,
            endTime: end,
            title: title,
            source: source,
            disposition: disposition,
            qualityScore: quality
        )
    }

    private func plan(
        chapters: [ChapterEvidence],
        confidence: Double = 0.8,
        hash: String = "test-hash"
    ) -> ChapterPlan {
        ChapterPlan(
            episodeContentHash: hash,
            chapters: chapters,
            planConfidence: confidence,
            generatedAt: Date(timeIntervalSince1970: 1_700_000_000)
        )
    }

    @Test("mode .off returns modeGated")
    func modeOffReturnsModeGated() {
        let p = plan(chapters: [
            chapter(start: 0, end: 60, title: "Intro", disposition: .content)
        ])
        let outcome = ChapterPromptContextSelector.select(
            mode: .off,
            plan: p,
            windowStart: 10,
            windowEnd: 30
        )
        #expect(outcome == .modeGated)
    }

    @Test("mode .shadow returns modeGated")
    func modeShadowReturnsModeGated() {
        let p = plan(chapters: [
            chapter(start: 0, end: 60, title: "Intro", disposition: .content)
        ])
        let outcome = ChapterPromptContextSelector.select(
            mode: .shadow,
            plan: p,
            windowStart: 10,
            windowEnd: 30
        )
        #expect(outcome == .modeGated)
    }

    @Test("nil plan returns modeGated even when enabled")
    func nilPlanReturnsModeGated() {
        let outcome = ChapterPromptContextSelector.select(
            mode: .enabled,
            plan: nil,
            windowStart: 10,
            windowEnd: 30
        )
        #expect(outcome == .modeGated)
    }

    @Test("low plan confidence returns modeGated")
    func lowPlanConfidenceReturnsModeGated() {
        let p = plan(
            chapters: [chapter(start: 0, end: 60, title: "Intro", disposition: .content)],
            confidence: 0.2
        )
        let outcome = ChapterPromptContextSelector.select(
            mode: .enabled,
            plan: p,
            windowStart: 10,
            windowEnd: 30
        )
        #expect(outcome == .modeGated)
    }

    @Test("plan confidence at threshold injects context")
    func planConfidenceAtThresholdInjects() {
        let p = plan(
            chapters: [chapter(start: 0, end: 60, title: "Intro", disposition: .content)],
            confidence: ChapterPromptContext.minimumPlanConfidence
        )
        let outcome = ChapterPromptContextSelector.select(
            mode: .enabled,
            plan: p,
            windowStart: 10,
            windowEnd: 30
        )
        if case .injected = outcome {
            // ok
        } else {
            Issue.record("expected .injected, got \(outcome)")
        }
    }

    @Test("window with no overlap returns noChapterForWindow")
    func windowWithNoOverlapReturnsNoChapter() {
        let p = plan(chapters: [
            chapter(start: 0, end: 60, title: "Intro", disposition: .content)
        ])
        let outcome = ChapterPromptContextSelector.select(
            mode: .enabled,
            plan: p,
            windowStart: 100,
            windowEnd: 120
        )
        #expect(outcome == .noChapterForWindow)
    }

    @Test("straddling window picks the chapter with larger overlap")
    func straddlingWindowPicksLargerOverlap() {
        // Window [50, 100]: chapter A=[0,60]→10s overlap, chapter B=[60,200]→40s overlap.
        let p = plan(chapters: [
            chapter(start: 0, end: 60, title: "Chapter A", disposition: .content),
            chapter(start: 60, end: 200, title: "Chapter B", disposition: .adBreak)
        ])
        let outcome = ChapterPromptContextSelector.select(
            mode: .enabled,
            plan: p,
            windowStart: 50,
            windowEnd: 100
        )
        guard case let .injected(ctx) = outcome else {
            Issue.record("expected .injected")
            return
        }
        #expect(ctx.chapterIndex == 2) // 1-based
        #expect(ctx.totalChapters == 2)
        #expect(ctx.dispositionToken == "adBreak")
        #expect(ctx.previousDispositionToken == "content")
    }

    @Test("source-agnostic: creator chapter and inferred chapter both inject")
    func sourceAgnosticHandling() {
        let creator = chapter(
            start: 0, end: 60, title: "Sponsored break",
            disposition: .adBreak, source: .id3
        )
        let inferred = chapter(
            start: 60, end: 120, title: "Hosts chat",
            disposition: .content, source: .inferred
        )
        let p = plan(chapters: [creator, inferred])

        let creatorOutcome = ChapterPromptContextSelector.select(
            mode: .enabled, plan: p, windowStart: 10, windowEnd: 30
        )
        guard case let .injected(creatorCtx) = creatorOutcome else {
            Issue.record("creator chapter should inject")
            return
        }
        #expect(creatorCtx.dispositionToken == "adBreak")

        let inferredOutcome = ChapterPromptContextSelector.select(
            mode: .enabled, plan: p, windowStart: 70, windowEnd: 110
        )
        guard case let .injected(inferredCtx) = inferredOutcome else {
            Issue.record("inferred chapter should inject")
            return
        }
        #expect(inferredCtx.dispositionToken == "content")
        #expect(inferredCtx.previousDispositionToken == "adBreak")
    }

    @Test("open-ended last chapter still matches windows past its start")
    func openEndedLastChapterMatches() {
        // Last chapter has nil endTime — selector should treat it as
        // extending forward.
        let p = plan(chapters: [
            chapter(start: 0, end: 60, title: "First", disposition: .content),
            chapter(start: 60, end: nil, title: "Outro", disposition: .content)
        ])
        let outcome = ChapterPromptContextSelector.select(
            mode: .enabled, plan: p, windowStart: 1000, windowEnd: 1100
        )
        guard case let .injected(ctx) = outcome else {
            Issue.record("expected injection for window past last chapter start")
            return
        }
        #expect(ctx.chapterIndex == 2)
    }
}

@Suite("Chapter prompt diagnostic events round-trip")
struct ChapterPromptDiagnosticEventsTests {

    @Test("chapter_prompt_injected encodes and decodes")
    func chapterPromptInjectedRoundTrip() throws {
        let event = ChapterPhaseEvent.chapterPromptInjected(
            installID: UUID(),
            episodeId: "ep-1",
            timestamp: 12.5,
            chapterIndex: 4,
            totalChapters: 7,
            disposition: "adBreak",
            previousDisposition: "content",
            topicIncluded: true
        )
        let data = try JSONEncoder().encode(event)
        let decoded = try JSONDecoder().decode(ChapterPhaseEvent.self, from: data)
        #expect(decoded == event)
        #expect(decoded.eventType == .chapterPromptInjected)
    }

    @Test("chapter_prompt_dropped_budget encodes and decodes")
    func chapterPromptDroppedBudgetRoundTrip() throws {
        let event = ChapterPhaseEvent.chapterPromptDroppedBudget(
            installID: UUID(),
            episodeId: "ep-1",
            timestamp: 12.5,
            budgetTokens: 50,
            abbreviatedFormTokens: 64
        )
        let data = try JSONEncoder().encode(event)
        let decoded = try JSONDecoder().decode(ChapterPhaseEvent.self, from: data)
        #expect(decoded == event)
        #expect(decoded.eventType == .chapterPromptDroppedBudget)
    }

    @Test("chapter_prompt_no_chapter_for_window encodes and decodes")
    func chapterPromptNoChapterForWindowRoundTrip() throws {
        let event = ChapterPhaseEvent.chapterPromptNoChapterForWindow(
            installID: UUID(),
            episodeId: "ep-1",
            timestamp: 12.5,
            chapterCount: 5
        )
        let data = try JSONEncoder().encode(event)
        let decoded = try JSONDecoder().decode(ChapterPhaseEvent.self, from: data)
        #expect(decoded == event)
        #expect(decoded.eventType == .chapterPromptNoChapterForWindow)
    }
}

@Suite("FoundationModelClassifier prompt builders accept chapter context")
struct FoundationModelClassifierChapterContextTests {

    @Test("buildPrompt with nil context preserves byte-identical output")
    func buildPromptNilContextPreservesOutput() {
        let preamble = FoundationModelClassifier.coarsePromptPreamble()
        let withDefault = FoundationModelClassifier.buildPrompt(for: [])
        let withNil = FoundationModelClassifier.buildPrompt(for: [], chapterContext: nil)
        #expect(withDefault == preamble)
        #expect(withNil == preamble)
    }

    @Test("buildPrompt with chapter context inserts chapter line above transcript")
    func buildPromptWithContextInsertsLine() {
        let ctx = ChapterPromptContext(
            chapterIndex: 2,
            totalChapters: 5,
            dispositionToken: "adBreak",
            previousDispositionToken: "content",
            topicDescriptor: "Promo"
        )
        let prompt = FoundationModelClassifier.buildPrompt(for: [], chapterContext: ctx)
        #expect(prompt.contains("Chapter context: 2/5 adBreak."))
        #expect(prompt.contains("Prev: content."))
        // The chapter line must appear before the transcript open fence
        // so the model reads it as orienting context, not as transcript.
        if let chapterRange = prompt.range(of: "Chapter context:"),
           let openFenceRange = prompt.range(of: "<<<TRANSCRIPT>>>") {
            #expect(chapterRange.lowerBound < openFenceRange.lowerBound)
        } else {
            Issue.record("expected both chapter line and transcript fence in prompt")
        }
    }

    @Test("buildRefinementPrompt with nil context preserves byte-identical output")
    func buildRefinementPromptNilContextPreservesOutput() {
        let baseline = FoundationModelClassifier.buildRefinementPrompt(
            for: [],
            promptEvidence: [],
            maximumSpans: 2
        )
        let withNil = FoundationModelClassifier.buildRefinementPrompt(
            for: [],
            promptEvidence: [],
            maximumSpans: 2,
            chapterContext: nil
        )
        #expect(baseline == withNil)
    }

    @Test("buildRefinementPrompt with chapter context inserts line at top")
    func buildRefinementPromptWithContextInsertsLine() {
        let ctx = ChapterPromptContext(
            chapterIndex: 1,
            totalChapters: 3,
            dispositionToken: "content",
            previousDispositionToken: nil,
            topicDescriptor: nil
        )
        let prompt = FoundationModelClassifier.buildRefinementPrompt(
            for: [],
            promptEvidence: [],
            maximumSpans: 2,
            chapterContext: ctx
        )
        #expect(prompt.hasPrefix("Chapter context: 1/3 content."))
    }
}
