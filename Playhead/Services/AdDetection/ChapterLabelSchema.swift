// ChapterLabelSchema.swift
// playhead-au2v.1.7: `@Generable` schema for the chapter labeling FM call.
//
// One `SystemLanguageModel` call per candidate chapter region produces a
// `ChapterLabel` that maps to the existing `ChapterDisposition`
// (adBreak/content/ambiguous) AND preserves a richer raw taxonomy
// (`ChapterDispositionRaw`) so downstream consumers — coverage planning,
// FM prompt context, fusion — can use the more specific label without
// breaking existing `ChapterDisposition` consumers.
//
// Token budget: this schema is deliberately small. Every `@Guide`
// description string in an `@Generable` schema gets serialized to the
// per-call token budget (see `FoundationModelClassifier` "bd-34e schema
// trim" comment). Most descriptions are short and rely on the per-call
// prompt for orientation. The one exception is `disposition`
// (au2v.1.25): its guide carries terse ad-classification cues because
// the model otherwise mislabels blatant host-read sponsor copy as
// `content`; that guidance is load-bearing and intentionally kept on
// both the prompt and the guide. The enriched `disposition` guide adds
// roughly 50 tokens over the prior one-line form, so the schema now
// occupies on the order of ~200 tokens against the FM's per-call budget.
//
// Off-device build (Mac Catalyst / non-FM simulator): the raw enum
// stays available; the `@Generable` `ChapterLabel` is gated behind
// `canImport(FoundationModels)` and an off-device shim (a plain
// struct with the same shape) is provided so non-FM consumers still
// compile. The mapping helpers and `LabelingResult` live outside the
// `#if` so both targets see them.

import Foundation

#if canImport(FoundationModels)
import FoundationModels
#endif

// MARK: - ChapterDispositionRaw

/// Richer 7-case chapter taxonomy emitted by the FM. Maps onto the
/// existing 3-case `ChapterDisposition` via `mappedDisposition`. The raw
/// value is persisted alongside the mapped disposition so downstream
/// consumers (FM prompt builders, fusion) can reason about, e.g., a
/// host-read ad versus a programmatic ad versus an outro.
#if canImport(FoundationModels)
@available(iOS 26.0, *)
@Generable
enum ChapterDispositionRaw: String, Codable, Sendable, Equatable, CaseIterable {
    case intro
    case content
    case hostReadAd
    case programmaticAd
    case outro
    case recap
    case unclear
}
#else
enum ChapterDispositionRaw: String, Codable, Sendable, Equatable, CaseIterable {
    case intro
    case content
    case hostReadAd
    case programmaticAd
    case outro
    case recap
    case unclear
}
#endif

extension ChapterDispositionRaw {
    /// Project the rich taxonomy onto the existing `ChapterDisposition`
    /// adBreak/content/ambiguous so legacy consumers keep working.
    ///
    /// Mapping (locked by tests in `ChapterLabelingServiceTests`):
    /// - `.hostReadAd`, `.programmaticAd` → `.adBreak`
    /// - `.unclear`                       → `.ambiguous`
    /// - `.intro`, `.content`, `.outro`, `.recap` → `.content`
    var mappedDisposition: ChapterDisposition {
        switch self {
        case .hostReadAd, .programmaticAd:
            return .adBreak
        case .unclear:
            return .ambiguous
        case .intro, .content, .outro, .recap:
            return .content
        }
    }
}

// MARK: - ChapterLabel (Generable schema)

#if canImport(FoundationModels)

/// Schema-bound output for the chapter labeling FM call. Apple's
/// `LanguageModelSession.respond(to:generating:)` constrains the model
/// to produce values conforming to this structure, so we get strong
/// taxonomy + range guarantees without writing a parser.
///
/// Confidence is clamped to `[0, 1]` by the caller (see
/// `ChapterLabelingService`); the FM occasionally emits values
/// slightly outside the documented range or non-finite (NaN/Inf) on
/// malformed responses.
@available(iOS 26.0, *)
@Generable
struct ChapterLabel: Sendable, Codable, Equatable {

    /// One of the seven `ChapterDispositionRaw` cases. The
    /// `@Generable` machinery enforces the enum at decode time; an
    /// out-of-taxonomy literal (e.g. `"music"`) surfaces as a
    /// `LanguageModelSession.GenerationError.decodingFailure` and the
    /// caller coerces to `.unclear` with `LabelFailureMode.operational`.
    @Guide(description: "Chapter disposition. hostReadAd: host reads a sponsor/ad (brand mention, call-to-action, promo code, URL, 'brought to you by'). programmaticAd: inserted ad, often different production. intro/outro/recap/content: editorial. unclear: cannot tell.")
    var disposition: ChapterDispositionRaw

    /// Caller clamps to [0, 1].
    @Guide(description: "Model confidence in disposition, 0.0 to 1.0.")
    var confidence: Double

    /// Optional 1-3 word topic descriptor for `.content` chapters
    /// (e.g. "interview", "Q&A", "news recap"). May be nil for ad
    /// chapters or ambiguous regions.
    @Guide(description: "Brief topic descriptor (1-3 words). Null if not applicable.")
    var topicDescriptor: String?
}

#else

/// Off-device shim. The non-FM build path never invokes the FM, so the
/// `@Generable` annotations are unnecessary. The struct shape stays
/// identical so the mapping helpers and `LabelingResult` compile on
/// both targets.
struct ChapterLabel: Sendable, Codable, Equatable {
    var disposition: ChapterDispositionRaw
    var confidence: Double
    var topicDescriptor: String?
}

#endif

// MARK: - LabelFailureMode

/// Reason a labeling call did not produce a confident answer. The
/// distinction matters at the plan level (bead au2v.1.8): operational
/// failures are a system-distrust signal — too many of them and the
/// whole `ChapterPlan` gets dropped — while semantic unclears are
/// legitimate model output and must be preserved as information.
enum LabelFailureMode: String, Codable, Sendable, Equatable {
    /// FM call failed: timeout, rate-limit / concurrent-request throttling,
    /// transient unavailability, schema validation / decoding failure,
    /// out-of-taxonomy output, or any unexpected error type. The runner
    /// retries once with exponential backoff; if the retry also fails
    /// the result still carries `.operational`.
    case operational
    /// FM call succeeded and returned `.unclear`. The model is telling us
    /// it cannot tell — we keep that signal rather than discarding the
    /// chapter, and we do NOT retry (a second call is overwhelmingly
    /// likely to return the same answer).
    case semantic
}

// MARK: - LabelingResult

/// Output of `ChapterLabelingService.label(...)`.
///
/// `chapter` already carries the mapped `ChapterDisposition` and the
/// caller-supplied bounds, so plan-assembly callers can append it to
/// `ChapterPlan.chapters` directly. `labelDisposition` preserves the
/// 7-case taxonomy for richer downstream reasoning. `failureMode` is
/// nil only when the FM successfully returned a non-`.unclear` answer.
struct LabelingResult: Sendable, Equatable {
    /// The chapter evidence for plan assembly. `source = .inferred`,
    /// `qualityScore = label.confidence`, `disposition` = the mapped
    /// `ChapterDisposition`. The labeling service does not invent
    /// `startTime` / `endTime` — they come from the candidate region.
    let chapter: ChapterEvidence
    /// The 7-case raw taxonomy. Equals `.unclear` whenever
    /// `failureMode != nil` (operational coercion AND semantic .unclear).
    let labelDisposition: ChapterDispositionRaw
    /// Optional topic descriptor from the FM. Nil for `.unclear` /
    /// operational-failure rows; may be nil for `.adBreak` rows.
    let topicDescriptor: String?
    /// `nil` on confident success; `.semantic` when the FM returned
    /// `.unclear`; `.operational` when the FM call (including the one
    /// retry) failed for any operational reason.
    let failureMode: LabelFailureMode?
    /// Number of FM calls made. Always `1` on success, semantic
    /// `.unclear`, or non-retryable failure; `2` when the first call
    /// failed operationally and we retried.
    let attempts: Int
}
