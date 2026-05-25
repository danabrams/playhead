// ChapterLabelingService.swift
// playhead-au2v.1.7: FM-backed chapter labeler.
//
// Design:
//
// One `SystemLanguageModel` call per candidate chapter region with the
// `@Generable` schema in `ChapterLabelSchema.swift`. The FM emits a
// `ChapterLabel` (disposition + confidence + optional topic descriptor),
// which the service projects into a `LabelingResult` carrying both the
// rich 7-case `ChapterDispositionRaw` taxonomy AND a `ChapterEvidence`
// row whose `disposition` is the existing 3-case `ChapterDisposition`.
//
// Retry / timeout policy:
// - One retry on operational failure (timeout, rate-limit / concurrent
//   requests, transient FM unavailability, decoding / schema failure,
//   out-of-taxonomy literals, unknown errors).
// - Exponential backoff: 50ms then 200ms before the second attempt.
// - Hard timeout per call: 8 seconds (matches the existing FM call
//   budget used by `FoundationModelClassifier`).
// - Semantic `.unclear` (FM succeeded, said "I cannot tell") is NOT
//   retried — a second call is overwhelmingly likely to repeat the
//   answer and would just burn budget.
// - Out-of-range confidence values (NaN / Inf or outside [0, 1]) are
//   clamped, not rejected. A clamped confidence still counts as a
//   successful call.
// - Out-of-taxonomy disposition (the @Generable decoder rejects the
//   value with `decodingFailure`) is treated as operational.
//
// Concurrency:
// - The service does NOT manage FM concurrency. Apple's
//   `LanguageModelSession` enforces its own request serialization
//   (surfacing `.concurrentRequests` as a rate-limit-style error,
//   which the retry catches). The chapter generation phase
//   orchestrator (au2v.1.10/.12, planned) is responsible for
//   throttling parallel labeling calls; this service is intentionally
//   a leaf that handles ONE region at a time.
// - Per-call session: a fresh `LanguageModelSession` is constructed
//   per call. This mirrors the bd-34e Fix B pattern in
//   `PermissiveAdClassifier` — a shared session accumulates ~4000
//   tokens of conversation history and starts hitting
//   `exceededContextWindowSize` after a handful of exchanges, which
//   would cause spurious retries here.
//
// Failure-mode distinction:
// - `.operational` — FM did not produce a usable answer. Plan-level
//   gate (au2v.1.8) drops the WHOLE plan when the operational rate
//   exceeds 30 % (system-distrust signal).
// - `.semantic`    — FM produced a usable answer of `.unclear`. The
//   chapter is still emitted with `disposition = .ambiguous` and the
//   raw `.unclear` is preserved; the plan is NOT dropped.

import Foundation
import OSLog

#if canImport(FoundationModels)
import FoundationModels
#endif

// MARK: - ChapterLabelingCandidate

/// Input to `ChapterLabelingService.label(...)`. Carries the
/// candidate region's text and bounds plus the position metadata that
/// flows into the prompt.
struct ChapterLabelingCandidate: Sendable, Equatable {
    /// Region start in seconds from episode start.
    let startTime: TimeInterval
    /// Region end in seconds. `nil` is allowed for the last chapter
    /// (matches `ChapterEvidence.endTime` semantics).
    let endTime: TimeInterval?
    /// Concatenated transcript text for the region. Trimmed and
    /// truncated by the prompt builder to stay inside the FM's
    /// per-call token budget.
    let regionText: String
    /// 1-indexed chapter ordinal for the prompt context line.
    let chapterIndex: Int
    /// Total chapters in the candidate set (1-indexed denominator).
    let totalChapters: Int
    /// Disposition the previous chapter resolved to (or `nil` for the
    /// first chapter / when the previous label is unknown). Plumbed
    /// into the prompt as a one-token "Previous: <value>" hint.
    let previousDisposition: ChapterDispositionRaw?

    init(
        startTime: TimeInterval,
        endTime: TimeInterval?,
        regionText: String,
        chapterIndex: Int,
        totalChapters: Int,
        previousDisposition: ChapterDispositionRaw?
    ) {
        self.startTime = startTime
        self.endTime = endTime
        self.regionText = regionText
        self.chapterIndex = chapterIndex
        self.totalChapters = totalChapters
        self.previousDisposition = previousDisposition
    }
}

// MARK: - ChapterLabelingError

/// Internal error surface for the FM call layer. Public API never
/// throws — the service folds every error type into a `.operational`
/// `LabelingResult`.
enum ChapterLabelingError: Error, Sendable, Equatable {
    /// Per-call hard timeout elapsed before the FM responded.
    case timedOut
    /// FM signalled rate-limit / concurrent-requests pushback.
    case rateLimited
    /// FM returned schema-invalid output (decode/guide failure).
    case decodingFailure
    /// FM ran out of context window mid-call.
    case exceededContextWindow
    /// Catch-all for any other operational failure (refusal, transient
    /// unavailability, unknown error type). The service does not
    /// distinguish these because the retry policy is identical.
    case operational(String)
}

// MARK: - ChapterLabelingService

/// FM-backed chapter labeler. Stateless; safe to share across actors.
struct ChapterLabelingService: Sendable {

    // MARK: - Configuration

    /// Per-call hard timeout (seconds). The FM call races against
    /// `Task.sleep(for:)`; whichever finishes first wins.
    static let perCallTimeoutSeconds: Double = 8.0

    /// Sleep before the FIRST call. Always zero — the first call is
    /// dispatched immediately.
    static let initialBackoffNanos: UInt64 = 0
    /// Sleep between attempt 1 and attempt 2. Exponential schedule
    /// (50ms, 200ms) — the second value is the gap between attempt 2
    /// and a hypothetical attempt 3 if we ever raised the retry cap.
    /// We currently retry once, so only the 50ms gap is consumed.
    static let firstRetryBackoffNanos: UInt64 = 50_000_000   // 50ms
    /// Reserved for documentation and future expansion. Not used at
    /// the current `maxAttempts == 2` cap.
    static let secondRetryBackoffNanos: UInt64 = 200_000_000 // 200ms

    /// Total attempts allowed (initial + retries). 2 = retry once.
    static let maxAttempts: Int = 2

    /// Hard cap on prompt body — the schema overhead lives in the
    /// `@Generable` machinery; this cap protects the per-call token
    /// budget from a runaway region transcript.
    static let regionTextCharacterCap: Int = 1200

    /// Hard cap on topic descriptor length the FM is asked to emit.
    /// Mirrors the prompt instruction.
    static let topicDescriptorCharacterCap: Int = 64

    // MARK: - Dependencies

    /// FM call. Closure-injected so unit tests can stub the
    /// FoundationModels framework without an iOS-26 simulator. Raises
    /// any `ChapterLabelingError` (or other `Error`); the service
    /// classifies the error into a retry decision.
    typealias LabelCall = @Sendable (_ prompt: String) async throws -> ChapterLabel

    private let labelCall: LabelCall
    private let logger: Logger
    private let clock: any Clock<Duration>

    init(
        labelCall: @escaping LabelCall,
        logger: Logger = Logger(subsystem: "com.playhead", category: "ChapterLabelingService"),
        clock: any Clock<Duration> = ContinuousClock()
    ) {
        self.labelCall = labelCall
        self.logger = logger
        self.clock = clock
    }

    // MARK: - Live constructor

    #if canImport(FoundationModels)
    /// Live FM-backed constructor. Each invocation builds a fresh
    /// `LanguageModelSession` (see file header for rationale).
    @available(iOS 26.0, *)
    static func live(
        logger: Logger = Logger(subsystem: "com.playhead", category: "ChapterLabelingService")
    ) -> ChapterLabelingService {
        ChapterLabelingService(
            labelCall: { prompt in
                let session = LanguageModelSession(model: SystemLanguageModel.default)
                let response = try await session.respond(
                    to: prompt,
                    generating: ChapterLabel.self,
                    options: GenerationOptions(sampling: .greedy)
                )
                return response.content
            },
            logger: logger
        )
    }
    #endif

    // MARK: - Entry point

    /// Label one candidate chapter region. Returns a `LabelingResult`
    /// regardless of FM outcome; never throws. See file header for
    /// the retry / timeout / failure-mode policy.
    func label(_ candidate: ChapterLabelingCandidate) async -> LabelingResult {
        let prompt = Self.buildPrompt(for: candidate)

        var attempts = 0
        var lastError: ChapterLabelingError?

        while attempts < Self.maxAttempts {
            attempts += 1

            // Honor external cancellation between attempts. The FM
            // call's child tasks fold `CancellationError` into a
            // `.operational(...)` shape, so without this check the
            // retry loop would keep going after the parent Task was
            // cancelled.
            if Task.isCancelled {
                return self.operationalResult(
                    for: candidate,
                    attempts: attempts,
                    cause: "cancelled"
                )
            }

            // Per-attempt backoff (only meaningful before attempt 2+).
            if attempts > 1 {
                let backoff = Self.backoffNanos(forRetryNumber: attempts - 1)
                if backoff > 0 {
                    do {
                        try await self.clock.sleep(
                            for: .nanoseconds(Int(min(backoff, UInt64(Int.max))))
                        )
                    } catch {
                        // Cooperative cancellation during backoff —
                        // surface as operational and stop retrying.
                        return self.operationalResult(
                            for: candidate,
                            attempts: attempts,
                            cause: "cancelled during backoff"
                        )
                    }
                }
            }

            do {
                let label = try await runWithTimeout(prompt: prompt)

                // Successful FM call. Distinguish semantic `.unclear`
                // from a confident answer.
                if label.disposition == .unclear {
                    return Self.semanticResult(
                        for: candidate,
                        rawConfidence: label.confidence,
                        topicDescriptor: label.topicDescriptor,
                        attempts: attempts
                    )
                }
                return Self.successResult(
                    for: candidate,
                    label: label,
                    attempts: attempts
                )
            } catch let error as ChapterLabelingError {
                lastError = error
                logger.debug(
                    "chapter_label_attempt_failed attempt=\(attempts, privacy: .public) error=\(String(describing: error), privacy: .public)"
                )
                continue
            } catch is CancellationError {
                // Cancellation must propagate through the orchestrator
                // — but the public API never throws. We emit an
                // operational result and stop retrying.
                return self.operationalResult(
                    for: candidate,
                    attempts: attempts,
                    cause: "cancelled"
                )
            } catch {
                let mapped = Self.classify(error)
                lastError = mapped
                logger.debug(
                    "chapter_label_attempt_failed_unmapped attempt=\(attempts, privacy: .public) error=\(String(describing: error), privacy: .public)"
                )
                continue
            }
        }

        // Exhausted retries.
        let cause = lastError.map { String(describing: $0) } ?? "unknown"
        return self.operationalResult(
            for: candidate,
            attempts: attempts,
            cause: cause
        )
    }

    // MARK: - Timeout race

    /// Run the FM call against a per-call hard timeout. Whichever
    /// finishes first wins; the loser is cancelled.
    private func runWithTimeout(prompt: String) async throws -> ChapterLabel {
        try await withThrowingTaskGroup(of: TimeoutOutcome.self) { group in
            // FM call.
            let labelCall = self.labelCall
            group.addTask {
                do {
                    let label = try await labelCall(prompt)
                    return .label(label)
                } catch is CancellationError {
                    // Re-throw so the outer task group surfaces
                    // cancellation to the retry loop instead of
                    // burying it inside `.operational(...)`.
                    throw CancellationError()
                } catch {
                    return .error(Self.classify(error))
                }
            }
            // Timeout.
            let timeoutClock = self.clock
            group.addTask {
                do {
                    try await timeoutClock.sleep(
                        for: .seconds(Self.perCallTimeoutSeconds)
                    )
                    return .timedOut
                } catch is CancellationError {
                    // Two cancellation sources: (a) the FM-call branch
                    // won the race and the group is being torn down,
                    // (b) the parent Task was cancelled externally.
                    // In case (a), `group.next()` will already have
                    // returned the FM result and our value is
                    // discarded. In case (b), the FM call also gets
                    // cancelled and re-throws `CancellationError`
                    // (see above), so this branch never wins. Either
                    // way, returning `.timedOut` is safe — the FM
                    // branch dominates the race when both finish.
                    return .timedOut
                } catch {
                    return .timedOut
                }
            }

            defer { group.cancelAll() }

            guard let first = try await group.next() else {
                throw ChapterLabelingError.operational("empty task group")
            }
            switch first {
            case .label(let label):
                return label
            case .error(let err):
                throw err
            case .timedOut:
                throw ChapterLabelingError.timedOut
            }
        }
    }

    private enum TimeoutOutcome: Sendable {
        case label(ChapterLabel)
        case error(ChapterLabelingError)
        case timedOut
    }

    // MARK: - Error classification

    /// Project an arbitrary `Error` into a `ChapterLabelingError`. All
    /// FoundationModels-framework errors are folded into operational
    /// buckets; everything else becomes `.operational(...)`.
    static func classify(_ error: Error) -> ChapterLabelingError {
        if let labelingError = error as? ChapterLabelingError {
            return labelingError
        }

        #if canImport(FoundationModels)
        if #available(iOS 26.0, *),
           let generationError = error as? LanguageModelSession.GenerationError {
            switch generationError {
            case .rateLimited, .concurrentRequests:
                return .rateLimited
            case .decodingFailure, .unsupportedGuide:
                return .decodingFailure
            case .exceededContextWindowSize:
                return .exceededContextWindow
            case .refusal, .guardrailViolation, .assetsUnavailable, .unsupportedLanguageOrLocale:
                return .operational(String(describing: generationError))
            @unknown default:
                return .operational(String(describing: generationError))
            }
        }
        #endif

        return .operational(error.localizedDescription)
    }

    // MARK: - Backoff schedule

    /// Backoff sleep before the Nth retry (`retryNumber` is 1-indexed:
    /// 1 = before attempt 2, 2 = before attempt 3, ...). Returns 0
    /// for any retry beyond the configured schedule.
    static func backoffNanos(forRetryNumber retryNumber: Int) -> UInt64 {
        switch retryNumber {
        case 1: return firstRetryBackoffNanos
        case 2: return secondRetryBackoffNanos
        default: return 0
        }
    }

    // MARK: - Result construction

    /// Build the success `LabelingResult`. Confidence is clamped to
    /// `[0, 1]`; non-finite values fall back to 0. The `topicDescriptor`
    /// is sanitized exactly once and reused for both the chapter title
    /// and the `LabelingResult.topicDescriptor` so the two cannot
    /// diverge.
    private static func successResult(
        for candidate: ChapterLabelingCandidate,
        label: ChapterLabel,
        attempts: Int
    ) -> LabelingResult {
        let confidence = clampConfidence(label.confidence)
        let title = label.topicDescriptor.flatMap(sanitizedTitle)
        let chapter = ChapterEvidence(
            startTime: candidate.startTime,
            endTime: candidate.endTime,
            title: title,
            source: .inferred,
            disposition: label.disposition.mappedDisposition,
            qualityScore: Float(confidence)
        )
        return LabelingResult(
            chapter: chapter,
            labelDisposition: label.disposition,
            topicDescriptor: title,
            failureMode: nil,
            attempts: attempts
        )
    }

    /// Build a `LabelingResult` for a semantic `.unclear` answer. The
    /// chapter is still emitted (with `.ambiguous` mapped disposition);
    /// `failureMode == .semantic` so plan-level callers can count
    /// these without dropping the plan. As with `successResult`, the
    /// topic descriptor is sanitized once and reused so chapter title
    /// and result topicDescriptor stay aligned.
    private static func semanticResult(
        for candidate: ChapterLabelingCandidate,
        rawConfidence: Double,
        topicDescriptor: String?,
        attempts: Int
    ) -> LabelingResult {
        let confidence = clampConfidence(rawConfidence)
        let title = topicDescriptor.flatMap(sanitizedTitle)
        let chapter = ChapterEvidence(
            startTime: candidate.startTime,
            endTime: candidate.endTime,
            title: title,
            source: .inferred,
            disposition: ChapterDispositionRaw.unclear.mappedDisposition,
            qualityScore: Float(confidence)
        )
        return LabelingResult(
            chapter: chapter,
            labelDisposition: .unclear,
            topicDescriptor: title,
            failureMode: .semantic,
            attempts: attempts
        )
    }

    /// Build a `LabelingResult` for an operational failure. The
    /// chapter is emitted with `.ambiguous` mapped disposition,
    /// `qualityScore = 0`, and `labelDisposition = .unclear`. The
    /// `cause` is logged at debug level so dogfood diagnostics can
    /// distinguish (e.g.) a timeout-only chapter from a decoding-only
    /// one without exposing it on the public `LabelingResult` shape.
    private func operationalResult(
        for candidate: ChapterLabelingCandidate,
        attempts: Int,
        cause: String
    ) -> LabelingResult {
        logger.debug(
            "chapter_label_operational_result attempts=\(attempts, privacy: .public) cause=\(cause, privacy: .public)"
        )
        let chapter = ChapterEvidence(
            startTime: candidate.startTime,
            endTime: candidate.endTime,
            title: nil,
            source: .inferred,
            disposition: ChapterDispositionRaw.unclear.mappedDisposition,
            qualityScore: 0.0
        )
        return LabelingResult(
            chapter: chapter,
            labelDisposition: .unclear,
            topicDescriptor: nil,
            failureMode: .operational,
            attempts: attempts
        )
    }

    /// Clamp confidence into `[0, 1]`. Non-finite (NaN/Inf) → 0.
    private static func clampConfidence(_ raw: Double) -> Double {
        guard raw.isFinite else { return 0.0 }
        return max(0.0, min(1.0, raw))
    }

    /// Trim and truncate a topic descriptor for use as the
    /// `ChapterEvidence.title`. Returns nil for empty input.
    private static func sanitizedTitle(_ raw: String) -> String? {
        let trimmed = raw.trimmingCharacters(in: .whitespacesAndNewlines)
        guard !trimmed.isEmpty else { return nil }
        if trimmed.count <= topicDescriptorCharacterCap {
            return trimmed
        }
        return String(trimmed.prefix(topicDescriptorCharacterCap))
    }

    // MARK: - Prompt builder

    /// Build the per-call prompt. The fixed instruction scaffold
    /// (everything except the variable context line and region body) is
    /// kept terse; its size is pinned by
    /// `ChapterLabelingServicePromptTests.promptScaffoldStaysWithinTokenBudget`
    /// using the project's conservative `ChapterPromptContext.estimateTokens`
    /// (`ceil(chars / 3)`) model. The runaway-body guard is the
    /// `regionTextCharacterCap` truncation in `truncateRegionText`.
    /// The overall prompt structure is locked by
    /// `ChapterLabelingServicePromptTests`.
    ///
    /// au2v.1.25: the disposition guidance is the load-bearing fix.
    /// Without it the model labelled blatant host-read sponsor copy as
    /// `content` (ChapterLabelingDiagnosticTests, dogfood corpus). The
    /// taxonomy + sponsor-read cues are kept terse to respect the budget.
    static func buildPrompt(for candidate: ChapterLabelingCandidate) -> String {
        let prev: String = candidate.previousDisposition?.rawValue ?? "none"
        let body = truncateRegionText(candidate.regionText)
        return """
            Classify this podcast chapter region. Output disposition, confidence, brief topic descriptor.
            Disposition: intro, content, hostReadAd, programmaticAd, outro, recap, unclear.
            Sponsor reads are ads: a sponsor name, a call-to-action, a promo code, a URL, or "brought to you by". Use hostReadAd when the host reads it in their own voice, programmaticAd for an inserted ad (often different production). Else content.
            Context: chapter \(candidate.chapterIndex) of \(candidate.totalChapters). Previous: \(prev).
            Region transcript: \(body)
            """
    }

    /// Trim and hard-cap the region transcript so a runaway input
    /// cannot blow the token budget. Caller may pass an already-trimmed
    /// region; this is defense-in-depth.
    private static func truncateRegionText(_ raw: String) -> String {
        let trimmed = raw.trimmingCharacters(in: .whitespacesAndNewlines)
        if trimmed.count <= regionTextCharacterCap {
            return trimmed
        }
        return String(trimmed.prefix(regionTextCharacterCap))
    }
}
