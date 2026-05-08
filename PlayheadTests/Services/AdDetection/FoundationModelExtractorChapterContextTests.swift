// FoundationModelExtractorChapterContextTests.swift
// playhead-au2v.1.17: End-to-end tests for chapter-context injection in
// `FoundationModelExtractor.buildPrompt(evidenceText:windowStartTime:
// windowEndTime:chapterContext:)`.
//
// `FoundationModelExtractor.buildPrompt` lives behind
// `#if canImport(FoundationModels)` because the surrounding extractor
// type uses Apple's `FoundationModels` framework. Bead 16 widened the
// builder's visibility from `private` to `static internal` and added
// the optional `chapterContext` argument. We guard the entire suite
// with the same `canImport` so the test target compiles on hosts that
// lack `FoundationModels`.
//
// Bead 16 already shipped a small set of builder smoke tests in
// `ChapterPromptContextTests.swift` (byte-identical-when-nil and
// chapter-line-above-Transcript: assertions). This file fills the
// gaps the bead-16 suite does not cover:
//   - Builder output with NON-empty `evidenceText` preserves byte-
//     identical output when no chapter context is supplied — the
//     `.off` / `.shadow` parity guarantee from the acceptance criteria.
//   - The exact `Chapter context: i/n disposition. Prev: x. Topic: t.`
//     format flows verbatim through the extractor builder.
//   - First chapter omits the `Prev:` clause (matches the formatter
//     contract bead 16 settled on).
//   - Long topic descriptor is truncated; if even the topic-less
//     baseline does not fit, the chapter line is dropped (the builder
//     emits no chapter line when `format()` returns nil, no extra
//     blank lines).
//   - End-to-end selector → builder threading for all four
//     `ChapterSource` variants (`.id3`, `.pc20`, `.rssInline`,
//     `.inferred`).
//   - Window straddling two chapters: majority overlap chapter wins
//     and its disposition shows up in the rendered prompt line.
//   - `planConfidence < 0.3` results in no chapter line in the
//     extractor prompt.
//   - Cache-lookup edge cases: nil plan and no-overlap window both
//     route to the no-context baseline.
//   - The extractor's `Transcript:` content marker still anchors the
//     transcript section regardless of chapter-context insertion.
//
// All tests are hermetic — the prompt builder is a pure
// string-construction function, so we never touch the
// `LanguageModelSession`, never load ML models, and never do I/O.

import Foundation
import Testing
@testable import Playhead

#if canImport(FoundationModels)

// MARK: - Helpers

private func makeChapter(
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

private func makePlan(
    chapters: [ChapterEvidence],
    confidence: Double = 0.8,
    hash: String = "fmecx-hash"
) -> ChapterPlan {
    ChapterPlan(
        episodeContentHash: hash,
        chapters: chapters,
        planConfidence: confidence,
        generatedAt: Date(timeIntervalSince1970: 1_700_000_000)
    )
}

// MARK: - Extractor parity (.off / .shadow / nil context)

@Suite("FoundationModelExtractor.buildPrompt parity with chapter context")
struct FoundationModelExtractorBuildPromptParityTests {

    /// Non-empty `evidenceText`: omitting `chapterContext` and passing
    /// `nil` produce byte-identical output. Locks in the `.off` /
    /// `.shadow` parity guarantee for the extractor builder.
    @Test("non-empty evidenceText: nil context preserves byte-identical output")
    func nonEmptyEvidenceNilContextPreservesOutput() {
        let evidence = "brought to you by squarespace, the all-in-one website platform"
        let baseline = FoundationModelExtractor.buildPrompt(
            evidenceText: evidence,
            windowStartTime: 12.0,
            windowEndTime: 24.0
        )
        let withNil = FoundationModelExtractor.buildPrompt(
            evidenceText: evidence,
            windowStartTime: 12.0,
            windowEndTime: 24.0,
            chapterContext: nil
        )
        #expect(baseline == withNil)
        // The extractor's prompt always anchors the transcript with
        // `Transcript:`. This invariant must hold whether or not
        // chapter context is present.
        #expect(baseline.contains("Transcript:"))
        #expect(baseline.contains("brought to you by squarespace"))
    }

    /// With chapter context, the rendered chapter line lands before
    /// the `Transcript:` marker so the model reads it as orienting
    /// context above the transcript body.
    @Test("non-empty evidenceText: chapter line lands before the Transcript: marker")
    func contextLineLandsAboveTranscriptMarker() throws {
        let ctx = ChapterPromptContext(
            chapterIndex: 4,
            totalChapters: 7,
            dispositionToken: "adBreak",
            previousDispositionToken: "content",
            topicDescriptor: "Squarespace promo"
        )
        let prompt = FoundationModelExtractor.buildPrompt(
            evidenceText: "promo copy",
            windowStartTime: 100.0,
            windowEndTime: 130.0,
            chapterContext: ctx
        )
        let chapterLine = "Chapter context: 4/7 adBreak. Prev: content. Topic: Squarespace promo."
        #expect(prompt.contains(chapterLine))
        let chapterRange = try #require(prompt.range(of: chapterLine))
        let transcriptRange = try #require(prompt.range(of: "Transcript:"))
        #expect(chapterRange.lowerBound < transcriptRange.lowerBound)
        // The window-time framing line must still be present and must
        // appear before the chapter line (the extractor's structural
        // contract).
        let windowFramingRange = try #require(prompt.range(of: "100.0s to 130.0s"))
        #expect(windowFramingRange.lowerBound < chapterRange.lowerBound)
    }

    /// First-chapter rule: when `previousDispositionToken == nil`,
    /// the formatter omits the `Prev:` clause and the extractor
    /// builder must surface a chapter line WITHOUT `Prev:`.
    @Test("first chapter: extractor prompt omits the Prev clause from the chapter line")
    func firstChapterOmitsPrevClause() throws {
        let ctx = ChapterPromptContext(
            chapterIndex: 1,
            totalChapters: 3,
            dispositionToken: "content",
            previousDispositionToken: nil,
            topicDescriptor: "Cold open"
        )
        let prompt = FoundationModelExtractor.buildPrompt(
            evidenceText: "intro",
            windowStartTime: 0.0,
            windowEndTime: 10.0,
            chapterContext: ctx
        )
        #expect(prompt.contains("Chapter context: 1/3 content. Topic: Cold open."))
        let chapterLine = try #require(
            prompt.split(separator: "\n").map(String.init)
                .first { $0.hasPrefix("Chapter context:") }
        )
        #expect(!chapterLine.contains("Prev:"))
    }
}

// MARK: - Format string verbatim

@Suite("FoundationModelExtractor emits the canonical chapter-line format")
struct FoundationModelExtractorChapterLineFormatTests {

    @Test("extractor emits 'Chapter context: i/n disposition. Prev: x. Topic: t.'")
    func extractorEmitsCanonicalLine() {
        let ctx = ChapterPromptContext(
            chapterIndex: 5,
            totalChapters: 8,
            dispositionToken: "ambiguous",
            previousDispositionToken: "content",
            topicDescriptor: "Listener question"
        )
        let prompt = FoundationModelExtractor.buildPrompt(
            evidenceText: "evidence",
            windowStartTime: 1.0,
            windowEndTime: 2.0,
            chapterContext: ctx
        )
        #expect(prompt.contains(
            "Chapter context: 5/8 ambiguous. Prev: content. Topic: Listener question."
        ))
    }
}

// MARK: - Token-budget guard

@Suite("Chapter-context token-budget guard threading into the extractor builder")
struct FoundationModelExtractorChapterBudgetTests {

    /// A topic descriptor too large for the default 50-token budget
    /// gets truncated by the formatter. The extractor builder must
    /// still emit a chapter line, and that line must fit the budget.
    @Test("very long topic descriptor is truncated and fits the default budget")
    func longTopicTruncatedFromExtractorOutput() throws {
        let longTopic = String(repeating: "alpha beta gamma ", count: 25)
        let ctx = ChapterPromptContext(
            chapterIndex: 1,
            totalChapters: 2,
            dispositionToken: "content",
            previousDispositionToken: "content",
            topicDescriptor: longTopic
        )
        let prompt = FoundationModelExtractor.buildPrompt(
            evidenceText: "evidence",
            windowStartTime: 1.0,
            windowEndTime: 2.0,
            chapterContext: ctx
        )
        let chapterLine = try #require(
            prompt.split(separator: "\n").map(String.init)
                .first { $0.hasPrefix("Chapter context:") }
        )
        #expect(
            ChapterPromptContext.estimateTokens(of: chapterLine)
                <= ChapterPromptContext.defaultMaxTokens
        )
        #expect(chapterLine.hasPrefix("Chapter context: 1/2 content."))
    }

    /// When even the topic-less baseline does not fit the default
    /// token budget, the formatter returns `nil` and the extractor
    /// builder MUST drop the chapter line entirely, falling back to
    /// the no-context baseline output (no chapter line, no orphan
    /// blank line gap that wasn't there before).
    ///
    /// Drive the drop path with a dispositionToken padded to ~200
    /// characters — this pushes the baseline form past 50 tokens
    /// under the `ceil(chars/3)` estimator.
    @Test("over-budget baseline drops the chapter line entirely from extractor output")
    func overBudgetBaselineDropsChapterLineFromExtractorOutput() {
        let oversizedDisposition = String(repeating: "z", count: 200)
        let ctx = ChapterPromptContext(
            chapterIndex: 1,
            totalChapters: 2,
            dispositionToken: oversizedDisposition,
            previousDispositionToken: nil,
            topicDescriptor: nil
        )
        #expect(ctx.format() == nil)

        let baseline = FoundationModelExtractor.buildPrompt(
            evidenceText: "evidence body",
            windowStartTime: 5.0,
            windowEndTime: 10.0
        )
        let withOversizedCtx = FoundationModelExtractor.buildPrompt(
            evidenceText: "evidence body",
            windowStartTime: 5.0,
            windowEndTime: 10.0,
            chapterContext: ctx
        )
        #expect(baseline == withOversizedCtx)
        #expect(!withOversizedCtx.contains("Chapter context:"))
        #expect(!withOversizedCtx.contains(oversizedDisposition))
    }

    /// No-context baseline: exactly one `Transcript:` marker, no
    /// `Chapter context:` line, no extra blank-line gap. Locks in the
    /// extractor's structural shape so a future `chapterContext = nil`
    /// regression that introduces a phantom blank line is loud.
    @Test("no-context baseline emits exactly one Transcript: marker and no chapter line")
    func noContextBaselineSingleTranscriptMarker() {
        let prompt = FoundationModelExtractor.buildPrompt(
            evidenceText: "evidence body",
            windowStartTime: 5.0,
            windowEndTime: 10.0,
            chapterContext: nil
        )
        let occurrences = prompt.components(separatedBy: "Transcript:").count - 1
        #expect(occurrences == 1)
        #expect(!prompt.contains("Chapter context:"))
    }
}

// MARK: - Selector → extractor builder threading

@Suite("ChapterPromptContextSelector → FoundationModelExtractor builder threading")
struct FoundationModelExtractorSelectorThreadingTests {

    @Test("mode .off threads to byte-identical extractor output (no chapter line)")
    func modeOffByteIdenticalExtractorOutput() {
        let plan = makePlan(chapters: [
            makeChapter(start: 0, end: 60, title: "Intro", disposition: .content)
        ])
        let outcome = ChapterPromptContextSelector.select(
            mode: .off,
            plan: plan,
            windowStart: 10,
            windowEnd: 30
        )
        #expect(outcome == .modeGated)
        let baseline = FoundationModelExtractor.buildPrompt(
            evidenceText: "evidence",
            windowStartTime: 10.0,
            windowEndTime: 30.0
        )
        let withGated = FoundationModelExtractor.buildPrompt(
            evidenceText: "evidence",
            windowStartTime: 10.0,
            windowEndTime: 30.0,
            chapterContext: nil
        )
        #expect(baseline == withGated)
        #expect(!baseline.contains("Chapter context:"))
    }

    @Test("mode .shadow threads to byte-identical extractor output (no chapter line)")
    func modeShadowByteIdenticalExtractorOutput() {
        let plan = makePlan(chapters: [
            makeChapter(start: 0, end: 60, title: "Intro", disposition: .content)
        ])
        let outcome = ChapterPromptContextSelector.select(
            mode: .shadow,
            plan: plan,
            windowStart: 10,
            windowEnd: 30
        )
        #expect(outcome == .modeGated)
        let baseline = FoundationModelExtractor.buildPrompt(
            evidenceText: "evidence",
            windowStartTime: 10.0,
            windowEndTime: 30.0
        )
        let withGated = FoundationModelExtractor.buildPrompt(
            evidenceText: "evidence",
            windowStartTime: 10.0,
            windowEndTime: 30.0,
            chapterContext: nil
        )
        #expect(baseline == withGated)
    }

    @Test("low plan confidence threads to byte-identical extractor output")
    func lowPlanConfidenceByteIdenticalExtractorOutput() {
        let plan = makePlan(
            chapters: [makeChapter(start: 0, end: 60, title: "Intro", disposition: .content)],
            confidence: 0.29
        )
        let outcome = ChapterPromptContextSelector.select(
            mode: .enabled,
            plan: plan,
            windowStart: 10,
            windowEnd: 30
        )
        #expect(outcome == .modeGated)
        let baseline = FoundationModelExtractor.buildPrompt(
            evidenceText: "evidence",
            windowStartTime: 10.0,
            windowEndTime: 30.0
        )
        let withGated = FoundationModelExtractor.buildPrompt(
            evidenceText: "evidence",
            windowStartTime: 10.0,
            windowEndTime: 30.0,
            chapterContext: nil
        )
        #expect(baseline == withGated)
    }

    @Test("nil plan (cache miss) threads to byte-identical extractor output")
    func nilPlanByteIdenticalExtractorOutput() {
        let outcome = ChapterPromptContextSelector.select(
            mode: .enabled,
            plan: nil,
            windowStart: 10,
            windowEnd: 30
        )
        #expect(outcome == .modeGated)
        let baseline = FoundationModelExtractor.buildPrompt(
            evidenceText: "evidence",
            windowStartTime: 10.0,
            windowEndTime: 30.0
        )
        let withGated = FoundationModelExtractor.buildPrompt(
            evidenceText: "evidence",
            windowStartTime: 10.0,
            windowEndTime: 30.0,
            chapterContext: nil
        )
        #expect(baseline == withGated)
    }

    @Test("no chapter for window threads to byte-identical extractor output")
    func noChapterForWindowByteIdenticalExtractorOutput() {
        let plan = makePlan(chapters: [
            makeChapter(start: 0, end: 60, title: "Intro", disposition: .content)
        ])
        let outcome = ChapterPromptContextSelector.select(
            mode: .enabled,
            plan: plan,
            windowStart: 1000,
            windowEnd: 1100
        )
        #expect(outcome == .noChapterForWindow)
        let baseline = FoundationModelExtractor.buildPrompt(
            evidenceText: "evidence",
            windowStartTime: 1000.0,
            windowEndTime: 1100.0
        )
        let withNil = FoundationModelExtractor.buildPrompt(
            evidenceText: "evidence",
            windowStartTime: 1000.0,
            windowEndTime: 1100.0,
            chapterContext: nil
        )
        #expect(baseline == withNil)
    }

    /// Cache-lookup edge case: a plan with confidence above threshold
    /// but an EMPTY `chapters` list. Selector returns
    /// `.noChapterForWindow`; caller passes nil to the extractor;
    /// extractor output is byte-identical to today's.
    @Test("empty chapters in valid plan: noChapterForWindow + byte-identical extractor output")
    func emptyChaptersListNoChapterForWindowExtractorOutput() {
        let emptyPlan = makePlan(chapters: [], confidence: 0.9)
        let outcome = ChapterPromptContextSelector.select(
            mode: .enabled,
            plan: emptyPlan,
            windowStart: 10,
            windowEnd: 30
        )
        #expect(outcome == .noChapterForWindow)
        let baseline = FoundationModelExtractor.buildPrompt(
            evidenceText: "evidence",
            windowStartTime: 10.0,
            windowEndTime: 30.0
        )
        let withNil = FoundationModelExtractor.buildPrompt(
            evidenceText: "evidence",
            windowStartTime: 10.0,
            windowEndTime: 30.0,
            chapterContext: nil
        )
        #expect(baseline == withNil)
    }

    @Test("plan confidence at threshold (0.30) injects chapter line into extractor output")
    func planConfidenceAtThresholdInjects() {
        let plan = makePlan(
            chapters: [makeChapter(start: 0, end: 60, title: "Intro Chapter", disposition: .content)],
            confidence: ChapterPromptContext.minimumPlanConfidence
        )
        let outcome = ChapterPromptContextSelector.select(
            mode: .enabled,
            plan: plan,
            windowStart: 10,
            windowEnd: 30
        )
        guard case let .injected(ctx) = outcome else {
            Issue.record("expected .injected at confidence threshold, got \(outcome)")
            return
        }
        let prompt = FoundationModelExtractor.buildPrompt(
            evidenceText: "evidence",
            windowStartTime: 10.0,
            windowEndTime: 30.0,
            chapterContext: ctx
        )
        #expect(prompt.contains("Chapter context: 1/1 content."))
        #expect(prompt.contains("Topic: Intro Chapter."))
    }

    @Test("straddling window: extractor sees the majority-overlap chapter's disposition")
    func straddlingWindowMajorityOverlapInExtractor() {
        let plan = makePlan(chapters: [
            makeChapter(start: 0, end: 60, title: "A", disposition: .content),
            makeChapter(start: 60, end: 200, title: "B", disposition: .adBreak)
        ])
        let outcome = ChapterPromptContextSelector.select(
            mode: .enabled,
            plan: plan,
            windowStart: 50,
            windowEnd: 100
        )
        guard case let .injected(ctx) = outcome else {
            Issue.record("expected .injected, got \(outcome)")
            return
        }
        let prompt = FoundationModelExtractor.buildPrompt(
            evidenceText: "evidence",
            windowStartTime: 50.0,
            windowEndTime: 100.0,
            chapterContext: ctx
        )
        #expect(prompt.contains("Chapter context: 2/2 adBreak."))
        #expect(prompt.contains("Prev: content."))
    }
}

// MARK: - Source-agnostic: all four ChapterSource variants

@Suite("FoundationModelExtractor source-agnostic chapter rendering")
struct FoundationModelExtractorChapterSourceVariantsTests {

    @Test("source-agnostic: all four ChapterSource variants render the same chapter line")
    func sourceAgnosticIdenticalRender() {
        var rendered: [ChapterSource: String] = [:]
        for source in ChapterSource.allCases {
            let plan = makePlan(chapters: [
                makeChapter(
                    start: 0, end: 60,
                    title: "Sponsor break",
                    disposition: .adBreak,
                    source: source
                )
            ])
            let outcome = ChapterPromptContextSelector.select(
                mode: .enabled,
                plan: plan,
                windowStart: 10,
                windowEnd: 30
            )
            guard case let .injected(ctx) = outcome else {
                Issue.record("expected .injected for source=\(source.rawValue)")
                return
            }
            let prompt = FoundationModelExtractor.buildPrompt(
                evidenceText: "evidence",
                windowStartTime: 10.0,
                windowEndTime: 30.0,
                chapterContext: ctx
            )
            let chapterLine = prompt
                .split(separator: "\n")
                .map(String.init)
                .first { $0.hasPrefix("Chapter context:") } ?? ""
            rendered[source] = chapterLine
        }

        // Defensive: covers every variant explicitly so adding a new
        // `ChapterSource` case shows up here as a missing key.
        #expect(ChapterSource.allCases.count == 4)
        let expected = "Chapter context: 1/1 adBreak. Topic: Sponsor break."
        for source in ChapterSource.allCases {
            #expect(rendered[source] == expected,
                    "extractor source \(source.rawValue) did not produce expected chapter line")
        }
    }
}

#endif
