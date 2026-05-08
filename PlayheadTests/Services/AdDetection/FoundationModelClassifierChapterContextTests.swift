// FoundationModelClassifierChapterContextTests.swift
// playhead-au2v.1.17: End-to-end tests for chapter-context injection in
// `FoundationModelClassifier.buildPrompt(for:redactor:chapterContext:)`
// and `.buildRefinementPrompt(for:promptEvidence:maximumSpans:redactor:chapterContext:)`.
//
// Bead 16 already shipped `ChapterPromptContextTests.swift` covering the
// `ChapterPromptContext` value type, the formatter contract, the
// `ChapterPromptContextSelector` mode-gate / lookup behavior, and a few
// minimal builder smoke tests against EMPTY transcript segments.
//
// This file fills the gaps the bead-16 suite does not cover:
//   - Builder output with NON-EMPTY transcript segments (the only path
//     that ships in production) preserves byte-identical output when no
//     chapter context is supplied — the `.off` / `.shadow` parity
//     guarantee from the acceptance criteria.
//   - The exact compact format `Chapter context: i/n disposition.
//     Prev: x. Topic: t.` flows verbatim through both builders.
//   - First-chapter `Prev:` clause is omitted (matches the formatter
//     contract that bead 16 settled on; the bead spec's literal
//     `Prev: none.` is satisfied by the omission, not by literal "none").
//   - Long topic descriptors get truncated by the formatter; if even
//     the topic-less baseline does not fit, the chapter line is dropped
//     entirely and the prompt is unchanged from its no-context shape.
//   - End-to-end selector→builder threading for all four
//     `ChapterSource` variants (`.id3`, `.pc20`, `.rssInline`,
//     `.inferred`).
//   - Window straddling two chapters: the chapter with majority overlap
//     wins and its disposition shows up in the rendered prompt line.
//   - `planConfidence < 0.3` results in the selector returning
//     `.modeGated`, the caller passes `nil` to the builder, and the
//     prompt is byte-identical to the no-context baseline.
//   - Cache-lookup edge cases: nil plan and no-overlap window both
//     route to the no-context baseline.
//   - `mode = .off` and `.shadow` produce builder output byte-identical
//     to the pre-bead-16 prompt — the parity guarantee that protects
//     `.shadow`'s "detection behavior identical to .off" contract.
//
// All tests are hermetic — no `FoundationModels` framework calls, no
// I/O. The classifier prompt builders are pure string-construction
// functions, so unit-level assertions on their `String` return value
// are the highest-fidelity test layer for this contract.

import Foundation
import Testing
@testable import Playhead

// MARK: - Helpers

/// Build a synthetic `AdTranscriptSegment` with a single `TranscriptAtom`
/// holding `text`, anchored at `[startTime, endTime]` with the given
/// `segmentIndex`. The atom's identity fields are stable per-test so
/// builder output is deterministic.
private func makeSegment(
    index: Int,
    text: String,
    startTime: Double = 0.0,
    endTime: Double = 1.0
) -> AdTranscriptSegment {
    let key = TranscriptAtomKey(
        analysisAssetId: "asset-fmccx",
        transcriptVersion: "v1",
        atomOrdinal: index
    )
    let atom = TranscriptAtom(
        atomKey: key,
        contentHash: "hash-\(index)",
        startTime: startTime,
        endTime: endTime,
        text: text,
        chunkIndex: index
    )
    return AdTranscriptSegment(
        atoms: [atom],
        segmentIndex: index,
        boundaryReason: .startOfTranscript,
        boundaryConfidence: 1.0,
        segmentType: .speech
    )
}

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
    hash: String = "fmccx-hash"
) -> ChapterPlan {
    ChapterPlan(
        episodeContentHash: hash,
        chapters: chapters,
        planConfidence: confidence,
        generatedAt: Date(timeIntervalSince1970: 1_700_000_000)
    )
}

// MARK: - Coarse buildPrompt parity (.off / .shadow / nil context)

@Suite("FoundationModelClassifier.buildPrompt parity with chapter context")
struct FoundationModelClassifierBuildPromptParityTests {

    /// With non-empty segments, the no-context prompt must be byte-
    /// identical regardless of whether `chapterContext` is omitted or
    /// passed as `nil`. This locks in the `.off` / `.shadow` parity
    /// invariant: when the selector returns `.modeGated`, the caller
    /// passes `nil` and the model sees the pre-bead-16 prompt verbatim.
    @Test("non-empty segments: nil context preserves byte-identical output")
    func nonEmptySegmentsNilContextPreservesOutput() {
        let segments = [
            makeSegment(index: 0, text: "Hello listeners."),
            makeSegment(index: 1, text: "Today on the show.")
        ]
        let baseline = FoundationModelClassifier.buildPrompt(for: segments)
        let withNil = FoundationModelClassifier.buildPrompt(
            for: segments,
            chapterContext: nil
        )
        #expect(baseline == withNil)
    }

    /// The full coarse-pass prompt with non-empty segments must include
    /// the H14 wrapping (open + close fences) AND the line-ref-prefixed
    /// transcript lines exactly once each. This is the structural
    /// invariant the chapter-context insertion must not disturb.
    @Test("non-empty segments: prompt structure is preamble + lines + close fence")
    func nonEmptySegmentsHasExpectedStructure() {
        let segments = [
            makeSegment(index: 0, text: "first line"),
            makeSegment(index: 1, text: "second line")
        ]
        let prompt = FoundationModelClassifier.buildPrompt(for: segments)
        #expect(prompt.contains("<<<TRANSCRIPT>>>"))
        #expect(prompt.contains("<<<END TRANSCRIPT>>>"))
        #expect(prompt.contains("L0> \"first line\""))
        #expect(prompt.contains("L1> \"second line\""))
    }

    /// With non-empty segments, providing chapter context inserts the
    /// formatted chapter line ABOVE the open transcript fence. The
    /// transcript line content and order are unchanged.
    @Test("non-empty segments: context line lands above the open fence and below the line-ref instruction")
    func contextLineInsertedAboveOpenFence() throws {
        let ctx = ChapterPromptContext(
            chapterIndex: 3,
            totalChapters: 5,
            dispositionToken: "adBreak",
            previousDispositionToken: "content",
            topicDescriptor: "Squarespace promo"
        )
        let segments = [
            makeSegment(index: 0, text: "first line"),
            makeSegment(index: 1, text: "second line")
        ]
        let prompt = FoundationModelClassifier.buildPrompt(
            for: segments,
            chapterContext: ctx
        )
        let chapterLine = "Chapter context: 3/5 adBreak. Prev: content. Topic: Squarespace promo."
        #expect(prompt.contains(chapterLine))

        let chapterRange = try #require(prompt.range(of: chapterLine))
        let openFenceRange = try #require(prompt.range(of: "<<<TRANSCRIPT>>>"))
        let lineRefRange = try #require(prompt.range(of: "Each transcript line is prefixed with"))
        let firstSegmentRange = try #require(prompt.range(of: "L0>"))
        // Chapter line is BELOW the line-ref instruction (so the model
        // reads orienting context after it has been told how to read the
        // transcript) and ABOVE the open fence (so it is not mistaken
        // for transcript content).
        #expect(lineRefRange.lowerBound < chapterRange.lowerBound)
        #expect(chapterRange.lowerBound < openFenceRange.lowerBound)
        // And, of course, above any transcript line.
        #expect(chapterRange.lowerBound < firstSegmentRange.lowerBound)
    }

    /// First-chapter rule: the formatter omits the `Prev:` clause when
    /// `previousDispositionToken == nil`. The acceptance criterion's
    /// literal `Prev: none.` text is satisfied by this omission — the
    /// production formatter contract is "no clause", not the literal
    /// word "none".
    @Test("first chapter omits the Prev clause from the rendered prompt")
    func firstChapterOmitsPrevClause() throws {
        let ctx = ChapterPromptContext(
            chapterIndex: 1,
            totalChapters: 4,
            dispositionToken: "content",
            previousDispositionToken: nil,
            topicDescriptor: "Cold open"
        )
        let segments = [makeSegment(index: 0, text: "intro")]
        let prompt = FoundationModelClassifier.buildPrompt(
            for: segments,
            chapterContext: ctx
        )
        #expect(prompt.contains("Chapter context: 1/4 content. Topic: Cold open."))
        // Stricter: the FULL chapter context line must not contain
        // "Prev:". Look for the exact line so we don't false-positive
        // on transcript content that happens to contain the string.
        let lines = prompt.split(separator: "\n").map(String.init)
        let chapterLines = lines.filter { $0.hasPrefix("Chapter context:") }
        #expect(chapterLines.count == 1)
        #expect(chapterLines.first?.contains("Prev:") == false)
    }
}

// MARK: - Format string verbatim across builders

@Suite("FoundationModelClassifier prompt builders emit the canonical chapter-line format")
struct FoundationModelClassifierChapterLineFormatTests {

    @Test("coarse buildPrompt emits 'Chapter context: i/n disposition. Prev: x. Topic: t.'")
    func coarseBuildPromptEmitsCanonicalLine() {
        let ctx = ChapterPromptContext(
            chapterIndex: 2,
            totalChapters: 6,
            dispositionToken: "adBreak",
            previousDispositionToken: "content",
            topicDescriptor: "Acme deal"
        )
        let prompt = FoundationModelClassifier.buildPrompt(
            for: [],
            chapterContext: ctx
        )
        #expect(prompt.contains("Chapter context: 2/6 adBreak. Prev: content. Topic: Acme deal."))
    }

    @Test("refinement buildRefinementPrompt emits 'Chapter context: i/n disposition. Prev: x. Topic: t.'")
    func refinementBuildPromptEmitsCanonicalLine() {
        let ctx = ChapterPromptContext(
            chapterIndex: 4,
            totalChapters: 9,
            dispositionToken: "content",
            previousDispositionToken: "adBreak",
            topicDescriptor: "Guest interview"
        )
        let prompt = FoundationModelClassifier.buildRefinementPrompt(
            for: [],
            promptEvidence: [],
            maximumSpans: 3,
            chapterContext: ctx
        )
        #expect(prompt.contains("Chapter context: 4/9 content. Prev: adBreak. Topic: Guest interview."))
    }

    @Test("refinement: chapter line precedes both transcript lines and Evidence catalog")
    func refinementChapterLinePrecedesEvidence() throws {
        let ctx = ChapterPromptContext(
            chapterIndex: 1,
            totalChapters: 3,
            dispositionToken: "content",
            previousDispositionToken: nil,
            topicDescriptor: nil
        )
        let segment = makeSegment(index: 7, text: "evidence sample")
        let prompt = FoundationModelClassifier.buildRefinementPrompt(
            for: [segment],
            promptEvidence: [],
            maximumSpans: 2,
            chapterContext: ctx
        )
        #expect(prompt.hasPrefix("Chapter context: 1/3 content."))
        let chapterRange = try #require(prompt.range(of: "Chapter context:"))
        let transcriptRange = try #require(prompt.range(of: "L7>"))
        #expect(chapterRange.lowerBound < transcriptRange.lowerBound)
    }
}

// MARK: - Token-budget guard: long topic truncation, baseline drop

@Suite("Chapter-context token-budget guard threading into the classifier builders")
struct FoundationModelClassifierChapterBudgetTests {

    @Test("very long topic descriptor is truncated and chapter line still fits")
    func longTopicTruncatedFromBuilderOutput() throws {
        // Construct a topic guaranteed to exceed the default 50-token
        // budget under the conservative `ceil(chars/3)` estimator.
        let longTopic = String(repeating: "alpha beta ", count: 30)
        let ctx = ChapterPromptContext(
            chapterIndex: 1,
            totalChapters: 2,
            dispositionToken: "content",
            previousDispositionToken: "content",
            topicDescriptor: longTopic
        )
        let prompt = FoundationModelClassifier.buildPrompt(
            for: [],
            chapterContext: ctx
        )
        // The chapter line must be present (truncated) — the formatter
        // does not return nil here because the topic-less baseline fits.
        #expect(prompt.contains("Chapter context: 1/2 content."))

        // Extract the rendered chapter line and verify it fits the
        // default budget. This locks in the truncation contract end-to-
        // end — if a future formatter regresses and emits the un-
        // truncated topic, this test fails loudly.
        let chapterLine = try #require(
            prompt.split(separator: "\n").map(String.init)
                .first(where: { $0.hasPrefix("Chapter context:") })
        )
        #expect(
            ChapterPromptContext.estimateTokens(of: chapterLine)
                <= ChapterPromptContext.defaultMaxTokens
        )
    }

    /// When even the topic-less baseline does not fit the formatter's
    /// default token budget, the formatter returns `nil` and the
    /// builder MUST drop the chapter line entirely. The acceptance
    /// criterion phrases this as "if still over budget, dropped
    /// entirely with diagnostic" — the diagnostic is the caller's
    /// responsibility; the builder's contract is to emit no chapter
    /// line in this case.
    ///
    /// We drive the drop path through the public API by constructing a
    /// context whose baseline form (`Chapter context: i/n disp.`)
    /// already exceeds the default 50-token budget. A
    /// dispositionToken padded to ~200 characters pushes the baseline
    /// well past 50 tokens under the conservative `ceil(chars/3)`
    /// estimator, so `format()` returns `nil` and the builder must
    /// emit a prompt byte-identical to the no-context baseline.
    @Test("over-budget baseline drops the chapter line entirely from coarse builder output")
    func overBudgetBaselineDropsChapterLineFromCoarseOutput() {
        let oversizedDisposition = String(repeating: "x", count: 200)
        let ctx = ChapterPromptContext(
            chapterIndex: 1,
            totalChapters: 2,
            dispositionToken: oversizedDisposition,
            previousDispositionToken: nil,
            topicDescriptor: nil
        )
        // Sanity: the formatter at default budget cannot fit even the
        // topic-less baseline form for this oversized disposition, so
        // it returns `nil`.
        #expect(ctx.format() == nil)

        let segments = [makeSegment(index: 0, text: "audio")]
        let baseline = FoundationModelClassifier.buildPrompt(for: segments)
        let withOversizedCtx = FoundationModelClassifier.buildPrompt(
            for: segments,
            chapterContext: ctx
        )
        // Builder must drop the chapter line and emit prompt identical
        // to the no-context baseline.
        #expect(baseline == withOversizedCtx)
        #expect(!withOversizedCtx.contains("Chapter context:"))
        // Defensive: the oversized disposition token must not leak
        // into the prompt via any other path.
        #expect(!withOversizedCtx.contains(oversizedDisposition))
    }

    /// Same drop-path verification for the refinement builder.
    @Test("over-budget baseline drops the chapter line entirely from refinement builder output")
    func overBudgetBaselineDropsChapterLineFromRefinementOutput() {
        let oversizedDisposition = String(repeating: "y", count: 200)
        let ctx = ChapterPromptContext(
            chapterIndex: 1,
            totalChapters: 2,
            dispositionToken: oversizedDisposition,
            previousDispositionToken: nil,
            topicDescriptor: nil
        )
        #expect(ctx.format() == nil)

        let segments = [makeSegment(index: 0, text: "ad copy")]
        let baseline = FoundationModelClassifier.buildRefinementPrompt(
            for: segments,
            promptEvidence: [],
            maximumSpans: 1
        )
        let withOversizedCtx = FoundationModelClassifier.buildRefinementPrompt(
            for: segments,
            promptEvidence: [],
            maximumSpans: 1,
            chapterContext: ctx
        )
        #expect(baseline == withOversizedCtx)
        #expect(!withOversizedCtx.contains("Chapter context:"))
        #expect(!withOversizedCtx.contains(oversizedDisposition))
    }
}

// MARK: - End-to-end selector → builder threading

@Suite("ChapterPromptContextSelector → FoundationModelClassifier builder threading")
struct FoundationModelClassifierSelectorThreadingTests {

    /// `mode = .off` — selector returns `.modeGated`, caller passes
    /// `nil`, builder output is byte-identical to today's pre-bead-16
    /// prompt. This is the ".shadow detection behavior must equal .off"
    /// contract surface.
    @Test("mode .off threads to byte-identical builder output (no chapter line)")
    func modeOffByteIdenticalBuilderOutput() {
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
        // The caller's contract: on `.modeGated`, pass nil to the
        // builder.
        let segments = [makeSegment(index: 0, text: "intro audio")]
        let baseline = FoundationModelClassifier.buildPrompt(for: segments)
        let withGated = FoundationModelClassifier.buildPrompt(
            for: segments,
            chapterContext: nil
        )
        #expect(baseline == withGated)
        #expect(!baseline.contains("Chapter context:"))
    }

    @Test("mode .shadow threads to byte-identical builder output (no chapter line)")
    func modeShadowByteIdenticalBuilderOutput() {
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
        let segments = [makeSegment(index: 0, text: "intro audio")]
        let baseline = FoundationModelClassifier.buildPrompt(for: segments)
        let withGated = FoundationModelClassifier.buildPrompt(
            for: segments,
            chapterContext: nil
        )
        #expect(baseline == withGated)
        #expect(!baseline.contains("Chapter context:"))
    }

    /// `planConfidence < 0.3` — selector returns `.modeGated`, builder
    /// output is byte-identical to today's prompt.
    @Test("low plan confidence threads to byte-identical builder output")
    func lowPlanConfidenceByteIdenticalBuilderOutput() {
        let plan = makePlan(
            chapters: [makeChapter(start: 0, end: 60, title: "Intro", disposition: .content)],
            confidence: 0.29 // Just below the 0.30 minimum.
        )
        let outcome = ChapterPromptContextSelector.select(
            mode: .enabled,
            plan: plan,
            windowStart: 10,
            windowEnd: 30
        )
        #expect(outcome == .modeGated)
        let segments = [makeSegment(index: 0, text: "audio")]
        let baseline = FoundationModelClassifier.buildPrompt(for: segments)
        let withGated = FoundationModelClassifier.buildPrompt(
            for: segments,
            chapterContext: nil
        )
        #expect(baseline == withGated)
    }

    /// `nil plan` (cache miss) — selector returns `.modeGated`, builder
    /// output is byte-identical to today's prompt.
    @Test("nil plan (cache miss) threads to byte-identical builder output")
    func nilPlanByteIdenticalBuilderOutput() {
        let outcome = ChapterPromptContextSelector.select(
            mode: .enabled,
            plan: nil,
            windowStart: 10,
            windowEnd: 30
        )
        #expect(outcome == .modeGated)
        let segments = [makeSegment(index: 0, text: "audio")]
        let baseline = FoundationModelClassifier.buildPrompt(for: segments)
        let withGated = FoundationModelClassifier.buildPrompt(
            for: segments,
            chapterContext: nil
        )
        #expect(baseline == withGated)
    }

    /// Window does not overlap any chapter — selector returns
    /// `.noChapterForWindow`, caller's contract is to pass `nil` to the
    /// builder; builder output is byte-identical to today's prompt.
    @Test("no chapter for window threads to byte-identical builder output")
    func noChapterForWindowByteIdenticalBuilderOutput() {
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
        let segments = [makeSegment(index: 0, text: "audio")]
        let baseline = FoundationModelClassifier.buildPrompt(for: segments)
        let withNil = FoundationModelClassifier.buildPrompt(
            for: segments,
            chapterContext: nil
        )
        #expect(baseline == withNil)
    }

    @Test("plan confidence at threshold (0.30) injects chapter line into builder output")
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
        let prompt = FoundationModelClassifier.buildPrompt(
            for: [makeSegment(index: 0, text: "audio")],
            chapterContext: ctx
        )
        #expect(prompt.contains("Chapter context: 1/1 content."))
        #expect(prompt.contains("Topic: Intro Chapter."))
    }

    /// Window straddles two chapters — selector picks the chapter with
    /// majority overlap; the chosen chapter's disposition is the one
    /// rendered in the prompt's chapter line.
    @Test("straddling window: builder sees the majority-overlap chapter's disposition")
    func straddlingWindowMajorityOverlapInBuilder() {
        // Chapter A=[0,60], Chapter B=[60,200]. Window [50,100]:
        // 10s overlap with A, 40s overlap with B → B wins.
        let plan = makePlan(chapters: [
            makeChapter(start: 0, end: 60, title: "A title", disposition: .content),
            makeChapter(start: 60, end: 200, title: "B title", disposition: .adBreak)
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
        let prompt = FoundationModelClassifier.buildPrompt(
            for: [makeSegment(index: 0, text: "audio")],
            chapterContext: ctx
        )
        // Chapter B's disposition is "adBreak"; previous chapter A's
        // disposition is "content". The builder must surface BOTH.
        #expect(prompt.contains("Chapter context: 2/2 adBreak."))
        #expect(prompt.contains("Prev: content."))
        #expect(prompt.contains("Topic: B title."))
    }
}

// MARK: - Source-agnostic: all four ChapterSource variants

@Suite("ChapterSource variants thread through selector + builder identically")
struct FoundationModelClassifierChapterSourceVariantsTests {

    /// Each `ChapterSource` variant produces a chapter that, with all
    /// other fields equal, threads identically through the selector
    /// and the builder. The `source` field is NOT rendered in the
    /// prompt line — the rendered output depends only on disposition,
    /// title, and ordinals — so the four variants must produce the
    /// SAME prompt-line content for the same window.
    @Test("source-agnostic: id3 / pc20 / rssInline / inferred render the same chapter line")
    func sourceAgnosticIdenticalRender() {
        let segments = [makeSegment(index: 0, text: "audio")]
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
            let prompt = FoundationModelClassifier.buildPrompt(
                for: segments,
                chapterContext: ctx
            )
            // Pull out the chapter line for direct comparison.
            let chapterLine = prompt
                .split(separator: "\n")
                .map(String.init)
                .first(where: { $0.hasPrefix("Chapter context:") }) ?? ""
            rendered[source] = chapterLine
        }

        // All four variants must render the same chapter line. Cover
        // every variant explicitly so adding a new `ChapterSource` case
        // surfaces here as a missing key.
        #expect(ChapterSource.allCases.count == 4)
        let expected = "Chapter context: 1/1 adBreak. Topic: Sponsor break."
        for source in ChapterSource.allCases {
            #expect(rendered[source] == expected,
                    "source \(source.rawValue) did not produce expected chapter line")
        }
    }

    /// Mixed-source plans (creator chapter then inferred) flow through
    /// the selector with the right `Prev:` token regardless of source.
    @Test("mixed source plan: creator + inferred chapters surface adjacent dispositions")
    func mixedSourcePlanAdjacentDispositions() {
        let plan = makePlan(chapters: [
            makeChapter(
                start: 0, end: 60,
                title: "Sponsored break",
                disposition: .adBreak,
                source: .id3
            ),
            makeChapter(
                start: 60, end: 120,
                title: "Hosts chat",
                disposition: .content,
                source: .inferred
            )
        ])

        // Creator chapter window (0–60) injects with no Prev token.
        let creatorOutcome = ChapterPromptContextSelector.select(
            mode: .enabled, plan: plan, windowStart: 10, windowEnd: 30
        )
        guard case let .injected(creatorCtx) = creatorOutcome else {
            Issue.record("expected .injected for creator chapter window")
            return
        }
        let creatorPrompt = FoundationModelClassifier.buildPrompt(
            for: [makeSegment(index: 0, text: "audio")],
            chapterContext: creatorCtx
        )
        #expect(creatorPrompt.contains("Chapter context: 1/2 adBreak."))
        // First chapter — no Prev clause.
        let creatorChapterLine = creatorPrompt
            .split(separator: "\n").map(String.init)
            .first { $0.hasPrefix("Chapter context:") }
        #expect(creatorChapterLine?.contains("Prev:") == false)

        // Inferred chapter window (60–120) injects with Prev=adBreak.
        let inferredOutcome = ChapterPromptContextSelector.select(
            mode: .enabled, plan: plan, windowStart: 70, windowEnd: 110
        )
        guard case let .injected(inferredCtx) = inferredOutcome else {
            Issue.record("expected .injected for inferred chapter window")
            return
        }
        let inferredPrompt = FoundationModelClassifier.buildPrompt(
            for: [makeSegment(index: 0, text: "audio")],
            chapterContext: inferredCtx
        )
        #expect(inferredPrompt.contains("Chapter context: 2/2 content."))
        #expect(inferredPrompt.contains("Prev: adBreak."))
    }
}

// MARK: - Refinement-pass parity (.off / .shadow / nil context)

@Suite("FoundationModelClassifier.buildRefinementPrompt parity")
struct FoundationModelClassifierBuildRefinementPromptParityTests {

    @Test("non-empty segments + evidence: nil context preserves byte-identical refinement output")
    func nonEmptyRefinementNilContextPreservesOutput() {
        let segments = [
            makeSegment(index: 0, text: "first ref line"),
            makeSegment(index: 1, text: "second ref line")
        ]
        let baseline = FoundationModelClassifier.buildRefinementPrompt(
            for: segments,
            promptEvidence: [],
            maximumSpans: 4
        )
        let withNil = FoundationModelClassifier.buildRefinementPrompt(
            for: segments,
            promptEvidence: [],
            maximumSpans: 4,
            chapterContext: nil
        )
        #expect(baseline == withNil)
    }

    @Test("refinement: chapter line precedes line-ref-prefixed transcript lines")
    func refinementChapterLinePrecedesTranscript() throws {
        let ctx = ChapterPromptContext(
            chapterIndex: 2,
            totalChapters: 4,
            dispositionToken: "adBreak",
            previousDispositionToken: "content",
            topicDescriptor: nil
        )
        let segments = [
            makeSegment(index: 5, text: "ad copy"),
            makeSegment(index: 6, text: "more ad copy")
        ]
        let prompt = FoundationModelClassifier.buildRefinementPrompt(
            for: segments,
            promptEvidence: [],
            maximumSpans: 2,
            chapterContext: ctx
        )
        let chapterRange = try #require(prompt.range(of: "Chapter context:"))
        let firstSegmentRange = try #require(prompt.range(of: "L5>"))
        let returnRange = try #require(prompt.range(of: "Return up to 2 spans."))
        #expect(chapterRange.lowerBound < firstSegmentRange.lowerBound)
        #expect(firstSegmentRange.lowerBound < returnRange.lowerBound)
    }

    @Test("refinement: nil context does not synthesize a phantom chapter line")
    func refinementNilContextNoPhantomLine() {
        let segments = [makeSegment(index: 0, text: "transcript")]
        let prompt = FoundationModelClassifier.buildRefinementPrompt(
            for: segments,
            promptEvidence: [],
            maximumSpans: 1,
            chapterContext: nil
        )
        #expect(!prompt.contains("Chapter context:"))
    }
}
