// ChapterPromptContext.swift
// playhead-au2v.1.16: Compact chapter context that the FM prompt builders
// (`FoundationModelClassifier.buildPrompt`, `.buildRefinementPrompt`, and
// `FoundationModelExtractor.buildPrompt`) inject just before the
// transcript / evidence-catalog body of each per-window FM call.
//
// Output shape (~30–50 tokens of guidance for the model):
//   `Chapter context: 4/7 hostReadAd. Prev: content. Topic: <descriptor>.`
//
// Mode gate (caller-side): `.off` and `.shadow` modes never construct a
// `ChapterPromptContext`; only `.enabled` does, and only when the cached
// `ChapterPlan` has `planConfidence >= minimumPlanConfidence`. Below that
// threshold the plan is treated as untrustworthy and consumers proceed
// without chapter context.
//
// Source-agnostic: creator chapters (`.id3` / `.pc20` / `.rssInline`) and
// inferred chapters (`.inferred`) flow through the same path. The plan
// (`ChapterPlan.chapters`) currently persists only the mapped 3-case
// `ChapterDisposition` (adBreak / content / ambiguous); the 7-case
// `ChapterDispositionRaw` and the FM-supplied `topicDescriptor` live on
// `LabelingResult`, which is not persisted. Bead 16 deliberately consumes
// what the plan exposes today rather than widening already-merged building
// blocks. If a future bead adds the richer fields to `ChapterEvidence`,
// the formatter swaps trivially.
//
// Token-budget guard: see `format(maxTokens:)`. The compact form is well
// under 50 tokens for typical inputs, but a long episode title (used as
// the `topicDescriptor` surrogate) can push it past the cap; in that
// case the formatter truncates the topic, and if even the topic-less
// abbreviated form does not fit, the formatter returns `nil` so the
// caller drops chapter context entirely and emits the
// `chapter_prompt_dropped_budget` diagnostic.

import Foundation

// MARK: - ChapterPromptContext

/// Compact, model-facing summary of where a window sits inside the
/// episode's chapter plan. Fed to FM prompt builders as a single line
/// formatted by `format(maxTokens:)`.
struct ChapterPromptContext: Sendable, Equatable {

    /// One-based ordinal of `this` chapter within the plan's chapter
    /// list. `1...total`.
    let chapterIndex: Int
    /// Total number of chapters in the plan.
    let totalChapters: Int
    /// String token rendered for this chapter's disposition. The
    /// constructor encodes the source-agnostic mapping rule (creator
    /// chapters → mapped `ChapterDisposition.rawValue`; inferred
    /// chapters → same mapped raw value, since `ChapterEvidence` does
    /// not currently carry the 7-case raw taxonomy from the plan).
    let dispositionToken: String
    /// String token rendered for the previous chapter's disposition,
    /// or `nil` when this is the first chapter (in which case the
    /// formatter omits the `Prev:` clause entirely).
    let previousDispositionToken: String?
    /// Compact topic descriptor. `ChapterEvidence` does not currently
    /// persist `LabelingResult.topicDescriptor`, so the chapter `title`
    /// (when non-empty) is used as the descriptor surrogate. `nil`
    /// suppresses the `Topic:` clause entirely.
    let topicDescriptor: String?

    init(
        chapterIndex: Int,
        totalChapters: Int,
        dispositionToken: String,
        previousDispositionToken: String?,
        topicDescriptor: String?
    ) {
        self.chapterIndex = chapterIndex
        self.totalChapters = totalChapters
        self.dispositionToken = dispositionToken
        self.previousDispositionToken = previousDispositionToken
        self.topicDescriptor = topicDescriptor
    }

    // MARK: Constants

    /// Default token cap for the formatted line. ~50 tokens matches the
    /// bead spec's "30-50 token" budget; the formatter truncates the
    /// topic descriptor before exceeding this.
    static let defaultMaxTokens: Int = 50

    /// Plan-confidence floor below which the chapter plan is too
    /// untrustworthy to inject. Matches the bead spec's `>= 0.3`.
    static let minimumPlanConfidence: Double = 0.3

    // MARK: Formatting

    /// Render the context as a single prompt-ready line.
    ///
    /// Returns `nil` when the abbreviated baseline form (with the
    /// topic descriptor dropped entirely) still exceeds `maxTokens`.
    /// Callers that receive `nil` MUST emit the
    /// `chapter_prompt_dropped_budget` diagnostic and proceed without
    /// chapter context.
    ///
    /// Token estimation uses a conservative `ceil(chars / 3)` rule
    /// when no `tokenCount` callback is supplied, matching the
    /// fallback used by other on-device prompt-budget paths in this
    /// codebase. Callers that already have access to a real token
    /// counter (e.g. `LanguageModelSession.tokenCount(for:)`) may
    /// pass their own closure to avoid the worst-case approximation.
    func format(
        maxTokens: Int = ChapterPromptContext.defaultMaxTokens,
        tokenCount: ((String) -> Int)? = nil
    ) -> String? {
        let counter = tokenCount ?? { Self.estimateTokens(of: $0) }

        // 1) Try the full form.
        let full = renderLine(topic: topicDescriptor)
        if counter(full) <= maxTokens { return full }

        // 2) Try a truncated topic. Drop a trailing word per iteration
        //    when whitespace is available; otherwise halve. The loop
        //    ALWAYS makes strict progress (the truncated length must
        //    shrink each iteration) so a degenerate one-character or
        //    no-whitespace input cannot spin forever.
        if let topic = topicDescriptor, !topic.isEmpty {
            var truncated = topic
            while !truncated.isEmpty {
                let nextTruncated: String
                if let lastSpace = truncated.lastIndex(where: { $0.isWhitespace }) {
                    nextTruncated = String(truncated[..<lastSpace])
                        .trimmingCharacters(in: .whitespacesAndNewlines)
                } else {
                    // Hard halve. Guarantee strict progress: when the
                    // length is 1, the next length is 0 (loop exits).
                    let halved = truncated.count / 2
                    nextTruncated = String(truncated.prefix(halved))
                }
                // Defensive: if the truncation rule did not shrink the
                // string, stop rather than spin. This should not happen
                // given the rules above but guards against future edits.
                if nextTruncated.count >= truncated.count { break }
                truncated = nextTruncated
                if truncated.isEmpty { break }
                let candidate = renderLine(topic: truncated)
                if counter(candidate) <= maxTokens { return candidate }
            }
        }

        // 3) Drop topic entirely; emit baseline form.
        let baseline = renderLine(topic: nil)
        if counter(baseline) <= maxTokens { return baseline }

        // 4) Even the baseline does not fit — caller drops context.
        return nil
    }

    // MARK: - Private helpers

    private func renderLine(topic: String?) -> String {
        var parts: [String] = [
            "Chapter context: \(chapterIndex)/\(totalChapters) \(dispositionToken)."
        ]
        if let prev = previousDispositionToken {
            parts.append("Prev: \(prev).")
        }
        if let topic, !topic.isEmpty {
            parts.append("Topic: \(topic).")
        }
        return parts.joined(separator: " ")
    }

    /// Conservative `ceil(chars / 3)` token estimate. Used when no
    /// tokenizer is supplied; a real tokenizer is preferred when one
    /// is in scope.
    static func estimateTokens(of text: String) -> Int {
        guard !text.isEmpty else { return 0 }
        // Use scalar count rather than `count` so multi-byte
        // characters do not under-estimate.
        let chars = text.unicodeScalars.count
        return (chars + 2) / 3
    }
}

// MARK: - ChapterPromptContextSelector

/// Pure helpers for selecting and constructing a `ChapterPromptContext`
/// from a cached `ChapterPlan`. Kept separate from
/// `ChapterPromptContext` so the value type stays a plain DTO.
enum ChapterPromptContextSelector {

    /// Outcome of `select(for:windowStart:windowEnd:plan:mode:)`. The
    /// distinct cases let the caller emit the right diagnostic event
    /// (`injected`, `dropped_budget`, or `no_chapter_for_window`)
    /// without a separate predicate.
    enum SelectionOutcome: Sendable, Equatable {
        /// A `ChapterPromptContext` is available for the window. The
        /// caller should emit `chapter_prompt_injected` and pass the
        /// context to the prompt builder.
        case injected(ChapterPromptContext)
        /// Mode is `.off` / `.shadow`, OR the plan is `nil`, OR the
        /// plan's `planConfidence` is below the threshold. The caller
        /// proceeds without chapter context and does NOT emit a
        /// `chapter_prompt_*` diagnostic — the mode gate is silent by
        /// design.
        case modeGated
        /// Mode and confidence allow injection but no chapter overlaps
        /// the window's `[start, end]`. The caller emits
        /// `chapter_prompt_no_chapter_for_window` and proceeds without
        /// chapter context.
        case noChapterForWindow
    }

    /// Decide whether a window inside `plan` deserves chapter context
    /// and, if so, build the context.
    ///
    /// - Parameters:
    ///   - mode: The active `ChapterSignalMode`. `.off` / `.shadow`
    ///     short-circuit to `.modeGated`.
    ///   - plan: The cached `ChapterPlan` (or `nil` when the cache
    ///     missed). `nil` short-circuits to `.modeGated` so the
    ///     consumer behaves identically to today's no-plan path.
    ///   - windowStart: Inclusive start of the FM window in episode
    ///     seconds.
    ///   - windowEnd: Inclusive end of the FM window in episode
    ///     seconds. Must be `>= windowStart`; an inverted pair is
    ///     treated as zero-length at `windowStart`.
    ///   - minimumPlanConfidence: Confidence floor; defaults to
    ///     `ChapterPromptContext.minimumPlanConfidence`. Below this the
    ///     plan is treated as untrustworthy and the helper returns
    ///     `.modeGated` (caller proceeds silently — same behavior as
    ///     no-plan).
    /// - Returns: `.injected(...)` when a chapter overlaps the window
    ///   and the plan is trustworthy; `.noChapterForWindow` when the
    ///   plan exists but no chapter covers the window; `.modeGated`
    ///   otherwise.
    static func select(
        mode: ChapterSignalMode,
        plan: ChapterPlan?,
        windowStart: Double,
        windowEnd: Double,
        minimumPlanConfidence: Double = ChapterPromptContext.minimumPlanConfidence
    ) -> SelectionOutcome {
        // Mode gate first — silent (no diagnostic) when the consumer
        // is configured off/shadow.
        guard mode.consumersReadChapterPlan else { return .modeGated }
        guard let plan else { return .modeGated }
        guard plan.planConfidence >= minimumPlanConfidence else {
            return .modeGated
        }

        let chapters = plan.chapters
        guard !chapters.isEmpty else { return .noChapterForWindow }

        let safeEnd = max(windowEnd, windowStart)
        guard let chosenIndex = bestOverlapIndex(
            chapters: chapters,
            windowStart: windowStart,
            windowEnd: safeEnd
        ) else {
            return .noChapterForWindow
        }

        let chapter = chapters[chosenIndex]
        let prevToken: String?
        if chosenIndex > 0 {
            prevToken = dispositionToken(for: chapters[chosenIndex - 1])
        } else {
            prevToken = nil
        }

        let context = ChapterPromptContext(
            chapterIndex: chosenIndex + 1,
            totalChapters: chapters.count,
            dispositionToken: dispositionToken(for: chapter),
            previousDispositionToken: prevToken,
            topicDescriptor: topicDescriptor(for: chapter)
        )
        return .injected(context)
    }

    /// Find the chapter with the largest overlap with
    /// `[windowStart, windowEnd]`. A window straddling two chapters
    /// returns the chapter with the larger overlap (ties resolve to
    /// the earlier chapter for determinism). Returns `nil` when no
    /// chapter overlaps the window at all.
    ///
    /// O(N) over chapters; the caller is expected to be `O(windows)`,
    /// so the overall behavior is O(windows × chapters). Real-episode
    /// chapter counts are small (≤ ~30), so an explicit binary search
    /// is not yet warranted.
    static func bestOverlapIndex(
        chapters: [ChapterEvidence],
        windowStart: Double,
        windowEnd: Double
    ) -> Int? {
        guard !chapters.isEmpty else { return nil }

        var bestIndex: Int?
        var bestOverlap: Double = 0.0

        for (idx, chapter) in chapters.enumerated() {
            let chStart = chapter.startTime
            // Treat open-ended chapters (no endTime) as extending to
            // `+∞` for the overlap calculation. We clamp via
            // `windowEnd` below so the result remains finite.
            let chEnd: Double
            if let end = chapter.endTime, end > chStart, end.isFinite {
                chEnd = end
            } else if chapter.endTime == nil, chStart.isFinite {
                chEnd = .greatestFiniteMagnitude
            } else {
                continue
            }
            guard chStart.isFinite else { continue }

            let overlapStart = max(chStart, windowStart)
            let overlapEnd = min(chEnd, windowEnd)
            let overlap = overlapEnd - overlapStart
            // Use a strict `>` so the earliest chapter wins on ties
            // (deterministic, matches the bead's "the earlier chapter"
            // tie-break implication).
            if overlap > bestOverlap {
                bestOverlap = overlap
                bestIndex = idx
            }
        }

        // Edge case: a zero-length window (`windowStart == windowEnd`)
        // produces an overlap of zero with every chapter and the loop
        // above never sets `bestIndex`. Fall back to the chapter that
        // contains the point, if any, so the consumer still sees the
        // right local context.
        if bestIndex == nil, windowStart == windowEnd {
            for (idx, chapter) in chapters.enumerated() {
                let chStart = chapter.startTime
                let chEnd = chapter.endTime ?? .greatestFiniteMagnitude
                guard chStart.isFinite else { continue }
                if chStart <= windowStart, windowStart <= chEnd {
                    return idx
                }
            }
        }

        return bestIndex
    }

    // MARK: - Token helpers

    /// Map a chapter to the disposition token rendered in the prompt
    /// line. Source-agnostic: the same mapping covers creator
    /// (`.id3` / `.pc20` / `.rssInline`) and inferred (`.inferred`)
    /// chapters because `ChapterEvidence.disposition` is the only
    /// disposition the plan persists. The 7-case
    /// `ChapterDispositionRaw` taxonomy lives on
    /// `ChapterLabelSchema.LabelingResult` and is not currently
    /// preserved through the plan; if a future bead persists it on
    /// `ChapterEvidence`, swap this helper to return the richer token
    /// (e.g. `hostReadAd`) for inferred chapters while keeping the
    /// 3-case fallback for creator chapters that lack the richer
    /// taxonomy.
    static func dispositionToken(for chapter: ChapterEvidence) -> String {
        chapter.disposition.rawValue
    }

    /// Topic descriptor surrogate. The persisted `ChapterEvidence` does
    /// not carry `LabelingResult.topicDescriptor`, so the chapter's
    /// `title` is used as the closest available descriptor. Returns
    /// `nil` when the title is empty or whitespace-only — in that case
    /// the formatter omits the `Topic:` clause entirely.
    static func topicDescriptor(for chapter: ChapterEvidence) -> String? {
        guard let title = chapter.title else { return nil }
        let trimmed = title.trimmingCharacters(in: .whitespacesAndNewlines)
        return trimmed.isEmpty ? nil : trimmed
    }
}
