import Foundation
import OSLog

#if canImport(FoundationModels)
import FoundationModels
#endif

// MARK: - Coarse screening schema

// bd-34e schema trim:
//
// Apple's `LanguageModelSession` charges the serialized `@Generable` schema
// against the per-call token budget. Every `@Guide` description string,
// every nested type, and every enum case shows up as fixed overhead on
// EVERY coarse / refinement call. The on-device benchmark from Phase 3
// observed ~3700 tokens of fixed overhead per coarse window, which left
// only ~300–400 tokens for actual transcript content and forced the
// planner into 2–3-segment windows that cut ads in half.
//
// Mitigation: drop every `@Guide` description string from the load-bearing
// schemas, drop fields the runner does not consume, and strip `@Generable`
// from helper enums that are no longer schema-visible. The slim
// `@Generable` types remain the wire shape Apple sees on each call;
// the rich `RefinedAdSpan` / `FMCoarseWindowOutput` types the runner
// consumes are derived from them with sensible defaults for the dropped
// fields.
//
// Backward compatibility: a small number of test fixtures outside this
// module's owned files construct `SpanRefinementSchema` with the legacy
// `alternativeExplanation` / `reasonTags` keyword arguments. We keep an
// extension init below that accepts (and silently ignores) those legacy
// arguments so non-owned tests keep compiling without modification.
#if canImport(FoundationModels)
// `TranscriptQuality` is computed deterministically on-device by
// `TranscriptQualityEstimator` and was never part of `CoarseScreeningSchema`.
// It used to carry `@Generable` for legacy reasons; dropping the conformance
// removes ~3 enum cases and the type's framing from any schema Apple
// happens to discover via reflection.
enum TranscriptQuality: String, Sendable, Codable, Hashable {
    case good
    case degraded
    case unusable
}

@available(iOS 26.0, *)
@Generable
enum CoarseDisposition: String, Sendable, Codable, Hashable {
    case noAds
    case containsAd
    case uncertain
    case abstain
}

@available(iOS 26.0, *)
@Generable
enum CertaintyBand: String, Sendable, Codable, Hashable {
    case weak
    case moderate
    case strong
}

@available(iOS 26.0, *)
@Generable
struct CoarseSupportSchema: Sendable, Codable, Hashable {
    var supportLineRefs: [Int]
    var certainty: CertaintyBand
}

// MARK: - CoarseScreeningSchema
//
// `transcriptQuality` is intentionally NOT part of this @Generable schema.
// Quality is computed deterministically on-device from the transcript via
// `TranscriptQualityEstimator`; asking the model to also emit it wastes
// tokens and any value the model returns is ignored. The deterministic
// quality is stored on `FMCoarseWindowOutput` instead.
@available(iOS 26.0, *)
@Generable
struct CoarseScreeningSchema: Sendable, Codable, Hashable {
    var disposition: CoarseDisposition
    var support: CoarseSupportSchema?
}

@available(iOS 26.0, *)
@Generable
enum CommercialIntent: String, Sendable, Codable, Hashable {
    case paid
    case owned
    case affiliate
    case organic
    case unknown
}

@available(iOS 26.0, *)
@Generable
enum Ownership: String, Sendable, Codable, Hashable {
    case thirdParty
    case show
    case network
    case guest
    case unknown
}

@available(iOS 26.0, *)
@Generable
enum BoundaryPrecision: String, Sendable, Codable, Hashable {
    case rough
    case usable
    case precise
}

// `AlternativeExplanation` and `ReasonTag` are no longer part of any
// `@Generable` schema (the FM no longer emits them — the runner does not
// persist either). They remain as plain Swift enums so `RefinedAdSpan`
// can default them and `sanitizeReasonTags` can keep its banner-tag
// filtering helper, but they cost zero schema tokens.
enum AlternativeExplanation: String, Sendable, Codable, Hashable {
    case none
    case editorialContext
    case guestPromotion
    case showCredits
    case unknown
}

enum ReasonTag: String, Sendable, Codable, Hashable {
    case callToAction
    case urlMention
    case promoCode
    case disclosure
    case brandMention
    case crossPromoLanguage
    case hostReadPitch
    case guestPlug
}

@available(iOS 26.0, *)
@Generable
enum EvidenceAnchorKind: String, Sendable, Codable, Hashable {
    case url
    case promoCode
    case ctaPhrase
    case disclosurePhrase
    case brandSpan
}

@available(iOS 26.0, *)
@Generable
struct EvidenceAnchorSchema: Sendable, Codable, Hashable {
    var evidenceRef: Int?
    var lineRef: Int
    var kind: EvidenceAnchorKind
    var certainty: CertaintyBand
}

@available(iOS 26.0, *)
@Generable
struct SpanRefinementSchema: Sendable, Codable, Hashable {
    var commercialIntent: CommercialIntent
    var ownership: Ownership
    var firstLineRef: Int
    var lastLineRef: Int
    var certainty: CertaintyBand
    var boundaryPrecision: BoundaryPrecision
    var evidenceAnchors: [EvidenceAnchorSchema]
}

@available(iOS 26.0, *)
@Generable
struct RefinementWindowSchema: Sendable, Codable, Hashable {
    var spans: [SpanRefinementSchema]
}
#else
enum TranscriptQuality: String, Sendable, Codable, Hashable {
    case good
    case degraded
    case unusable
}

enum CoarseDisposition: String, Sendable, Codable, Hashable {
    case noAds
    case containsAd
    case uncertain
    case abstain
}

enum CertaintyBand: String, Sendable, Codable, Hashable {
    case weak
    case moderate
    case strong
}

struct CoarseSupportSchema: Sendable, Codable, Hashable {
    var supportLineRefs: [Int]
    var certainty: CertaintyBand
}

struct CoarseScreeningSchema: Sendable, Codable, Hashable {
    var disposition: CoarseDisposition
    var support: CoarseSupportSchema?
}

enum CommercialIntent: String, Sendable, Codable, Hashable {
    case paid
    case owned
    case affiliate
    case organic
    case unknown
}

enum Ownership: String, Sendable, Codable, Hashable {
    case thirdParty
    case show
    case network
    case guest
    case unknown
}

enum BoundaryPrecision: String, Sendable, Codable, Hashable {
    case rough
    case usable
    case precise
}

enum AlternativeExplanation: String, Sendable, Codable, Hashable {
    case none
    case editorialContext
    case guestPromotion
    case showCredits
    case unknown
}

enum ReasonTag: String, Sendable, Codable, Hashable {
    case callToAction
    case urlMention
    case promoCode
    case disclosure
    case brandMention
    case crossPromoLanguage
    case hostReadPitch
    case guestPlug
}

enum EvidenceAnchorKind: String, Sendable, Codable, Hashable {
    case url
    case promoCode
    case ctaPhrase
    case disclosurePhrase
    case brandSpan
}

struct EvidenceAnchorSchema: Sendable, Codable, Hashable {
    var evidenceRef: Int?
    var lineRef: Int
    var kind: EvidenceAnchorKind
    var certainty: CertaintyBand
}

struct SpanRefinementSchema: Sendable, Codable, Hashable {
    var commercialIntent: CommercialIntent
    var ownership: Ownership
    var firstLineRef: Int
    var lastLineRef: Int
    var certainty: CertaintyBand
    var boundaryPrecision: BoundaryPrecision
    var evidenceAnchors: [EvidenceAnchorSchema]
}

struct RefinementWindowSchema: Sendable, Codable, Hashable {
    var spans: [SpanRefinementSchema]
}
#endif

// MARK: - Backward-compatible SpanRefinementSchema init
//
// bd-34e schema trim: a small number of test fixtures outside this file
// (most notably `BackfillJobRunnerTests.swift`) construct
// `SpanRefinementSchema` with the legacy `alternativeExplanation:` and
// `reasonTags:` keyword arguments. Those fields are no longer part of the
// `@Generable` schema, but to keep non-owned tests compiling without
// edits we expose an overload that accepts them and silently ignores
// them. Production code paths use the slim memberwise init.
extension SpanRefinementSchema {
    init(
        commercialIntent: CommercialIntent,
        ownership: Ownership,
        firstLineRef: Int,
        lastLineRef: Int,
        certainty: CertaintyBand,
        boundaryPrecision: BoundaryPrecision,
        evidenceAnchors: [EvidenceAnchorSchema],
        alternativeExplanation: AlternativeExplanation,
        reasonTags: [ReasonTag]
    ) {
        // alternativeExplanation and reasonTags are intentionally dropped
        // from the slim @Generable schema (bd-34e). They are no longer
        // emitted by the FM nor persisted by the runner; defaulted on
        // `RefinedAdSpan` instead.
        _ = alternativeExplanation
        _ = reasonTags
        self.init(
            commercialIntent: commercialIntent,
            ownership: ownership,
            firstLineRef: firstLineRef,
            lastLineRef: lastLineRef,
            certainty: certainty,
            boundaryPrecision: boundaryPrecision,
            evidenceAnchors: evidenceAnchors
        )
    }
}

// MARK: - Output

struct CoarsePassWindowPlan: Sendable, Equatable {
    let windowIndex: Int
    let lineRefs: [Int]
    let prompt: String
    let promptTokenCount: Int
    let startTime: Double
    let endTime: Double
    let transcriptQuality: TranscriptQuality
}

struct FMCoarseWindowOutput: Sendable, Equatable {
    let windowIndex: Int
    let lineRefs: [Int]
    let startTime: Double
    let endTime: Double
    let transcriptQuality: TranscriptQuality
    let screening: CoarseScreeningSchema
    let latencyMillis: Double
}

struct FMCoarseScanOutput: Sendable, Equatable {
    let status: SemanticScanStatus
    let windows: [FMCoarseWindowOutput]
    let latencyMillis: Double
    let prewarmHit: Bool
    /// bd-34e Fix B v3: per-window failure statuses captured when a window
    /// is abandoned by the smart-shrink retry loop but the rest of the
    /// pass continues. Empty when every window succeeded or when the
    /// pass aborted on a non-recoverable failure.
    let failedWindowStatuses: [SemanticScanStatus]

    init(
        status: SemanticScanStatus,
        windows: [FMCoarseWindowOutput],
        latencyMillis: Double,
        prewarmHit: Bool,
        failedWindowStatuses: [SemanticScanStatus] = []
    ) {
        self.status = status
        self.windows = windows
        self.latencyMillis = latencyMillis
        self.prewarmHit = prewarmHit
        self.failedWindowStatuses = failedWindowStatuses
    }
}

enum ZoomStopReason: String, Sendable, Codable, Hashable {
    case tokenBudget
    case ambiguityBudget
    case minimumSpan
}

struct PromptEvidenceEntry: Sendable {
    let entry: EvidenceEntry
    let lineRef: Int

    func renderForPrompt() -> String {
        "[E\(entry.evidenceRef)] \"\(entry.matchedText)\" (\(entry.category.rawValue), line \(lineRef))"
    }
}

struct RefinementWindowPlan: Sendable {
    let windowIndex: Int
    let sourceWindowIndex: Int
    let lineRefs: [Int]
    let focusLineRefs: [Int]
    let focusClusters: [[Int]]
    let prompt: String
    let promptTokenCount: Int
    let startTime: Double
    let endTime: Double
    let stopReason: ZoomStopReason
    let promptEvidence: [PromptEvidenceEntry]
}

struct ResolvedEvidenceAnchor: Sendable {
    let entry: EvidenceEntry?
    let lineRef: Int
    let kind: EvidenceCategory
    let certainty: CertaintyBand
    let resolutionSource: CommercialEvidenceResolutionSource
    let memoryWriteEligible: Bool
}

struct RefinedAdSpan: Sendable {
    let commercialIntent: CommercialIntent
    let ownership: Ownership
    let firstLineRef: Int
    let lastLineRef: Int
    let firstAtomOrdinal: Int
    let lastAtomOrdinal: Int
    let certainty: CertaintyBand
    let boundaryPrecision: BoundaryPrecision
    let resolvedEvidenceAnchors: [ResolvedEvidenceAnchor]
    let memoryWriteEligible: Bool
    let alternativeExplanation: AlternativeExplanation
    let reasonTags: [ReasonTag]
}

struct FMRefinementWindowOutput: Sendable {
    let windowIndex: Int
    let sourceWindowIndex: Int
    let lineRefs: [Int]
    let spans: [RefinedAdSpan]
    let latencyMillis: Double
}

struct FMRefinementScanOutput: Sendable {
    let status: SemanticScanStatus
    let windows: [FMRefinementWindowOutput]
    let latencyMillis: Double
    let prewarmHit: Bool
}

enum FoundationModelClassifierError: Error, Sendable, LocalizedError {
    case segmentExceedsTokenBudget(lineRef: Int, tokenCount: Int, budget: Int)
    case runtimeUnavailable(String)

    var errorDescription: String? {
        switch self {
        case let .segmentExceedsTokenBudget(lineRef, tokenCount, budget):
            "Transcript segment \(lineRef) needs \(tokenCount) tokens, exceeding the prompt budget of \(budget)."
        case let .runtimeUnavailable(reason):
            reason
        }
    }
}

// MARK: - Classifier

struct FoundationModelClassifier: Sendable {
    private enum CoarseResponseOutcome {
        case success([FMCoarseWindowOutput])
        case failure(SemanticScanStatus)
    }

    private enum RefinementResponseOutcome {
        case success(plan: RefinementWindowPlan, schema: RefinementWindowSchema)
        case failure(SemanticScanStatus)
    }

    struct Config: Sendable {
        let safetyMarginTokens: Int
        let coarseMaximumResponseTokens: Int
        let refinementMaximumResponseTokens: Int
        let zoomAmbiguityBudget: Int
        let minimumZoomSpanLines: Int
        let maximumRefinementSpansPerWindow: Int

        static let `default` = Config(
            safetyMarginTokens: 128,
            coarseMaximumResponseTokens: 96,
            refinementMaximumResponseTokens: 192,
            zoomAmbiguityBudget: 1,
            minimumZoomSpanLines: 2,
            maximumRefinementSpansPerWindow: 2
        )

        init(
            safetyMarginTokens: Int,
            coarseMaximumResponseTokens: Int,
            refinementMaximumResponseTokens: Int,
            zoomAmbiguityBudget: Int = 1,
            minimumZoomSpanLines: Int = 2,
            maximumRefinementSpansPerWindow: Int = 2
        ) {
            self.safetyMarginTokens = safetyMarginTokens
            self.coarseMaximumResponseTokens = coarseMaximumResponseTokens
            self.refinementMaximumResponseTokens = refinementMaximumResponseTokens
            self.zoomAmbiguityBudget = max(0, zoomAmbiguityBudget)
            self.minimumZoomSpanLines = max(1, minimumZoomSpanLines)
            self.maximumRefinementSpansPerWindow = max(1, maximumRefinementSpansPerWindow)
        }

        init(
            safetyMarginTokens: Int,
            maximumResponseTokens: Int
        ) {
            self.init(
                safetyMarginTokens: safetyMarginTokens,
                coarseMaximumResponseTokens: maximumResponseTokens,
                refinementMaximumResponseTokens: maximumResponseTokens * 2
            )
        }
    }

    struct Runtime: Sendable {
        struct Session: Sendable {
            let prewarm: @Sendable (_ promptPrefix: String) async -> Void
            let respondCoarse: @Sendable (_ prompt: String) async throws -> CoarseScreeningSchema
            let respondRefinement: @Sendable (_ prompt: String) async throws -> RefinementWindowSchema
        }

        let availabilityStatus: @Sendable (_ locale: Locale) async -> SemanticScanStatus?
        let contextSize: @Sendable () async -> Int
        let tokenCount: @Sendable (_ prompt: String) async throws -> Int
        let coarseSchemaTokenCount: @Sendable () async throws -> Int
        let refinementSchemaTokenCount: @Sendable () async throws -> Int
        let makeSession: @Sendable () async -> Session
    }

    private static let promptPrefix = "Classify ad content."
    private static let refinementPromptPrefix = "Refine ad spans."

    // H10: The native `model.tokenCount(for:)` API isn't available on iOS
    // 26.0–26.3, so we estimate. The previous `wordCount * 1.35` factor
    // under-counted on multi-byte / punctuation-dense input and could push us
    // past the real context window. We now use `wordCount * 2.0 + 16` plus a
    // safety slack representing the bumped 128 → 256 margin. Conservative on
    // purpose: false-large counts only cost prompt headroom; false-small
    // counts can crash the request with `exceededContextWindow`.
    static func fallbackTokenEstimate(for prompt: String) -> Int {
        let wordCount = prompt.split(whereSeparator: \.isWhitespace).count
        let estimate = Int(ceil(Double(wordCount) * 2.0)) + 16
        return max(1, estimate + 128)
    }
    private static let fallbackCoarseSchemaTokenEstimate = 128
    private static let fallbackRefinementSchemaTokenEstimate = 256

    /// Diagnostic payload emitted when the refinement pass catches a
    /// `SemanticScanStatus.decodingFailure` from the Foundation Models session.
    /// Captures every cheap-to-grab field that could narrow down bd-3h2 (the
    /// 50% refinement decode failure rate seen on real devices) without
    /// requiring another on-device benchmark run to reproduce.
    struct RefinementDecodeFailureDiagnostic: Sendable {
        let windowIndex: Int
        let sourceWindowIndex: Int
        let firstLineRef: Int?
        let lastLineRef: Int?
        let lineRefCount: Int
        let focusClusterCount: Int
        let promptTokenCount: Int
        let schemaName: String
        let stopReason: ZoomStopReason
        let status: SemanticScanStatus
        let errorDescription: String
        let errorDebugDescription: String
        let retryStage: RetryStage

        enum RetryStage: String, Sendable {
            case initial
            case shrinkRetry
            case backoffRetry
        }
    }

    /// Internal test hook so unit tests can observe refinement decode-failure
    /// diagnostics without scraping `os.Logger`. Mirrors the pattern used by
    /// `SemanticScanResult.decodeFailureObserver`. Production builds leave it
    /// nil; invoking it is a no-op.
    nonisolated(unsafe) static var refinementDecodeFailureObserver: (@Sendable (RefinementDecodeFailureDiagnostic) -> Void)?

    /// bd-34e diagnostic payload emitted around every coarse-pass window
    /// `respond` call: one event per submission attempt and one event per
    /// catch arm. Captures the prompt metadata an investigator needs to
    /// triage why Apple's safety classifier flagged a window without rerunning
    /// the on-device shadow benchmark blind. Mirrors
    /// `RefinementDecodeFailureDiagnostic` in shape and intent.
    struct CoarsePassWindowDiagnostic: Sendable {
        enum Kind: String, Sendable {
            case submit
            case error
            /// bd-34e Fix B v3: emitted before each smart-shrink retry
            /// attempt with the iteration number, the actual token count
            /// Apple reported, and the new target segment count.
            case smartShrinkAttempt
            /// bd-34e Fix B v3: emitted after each smart-shrink retry
            /// completes (success, retried again, or abandoned).
            case smartShrinkOutcome
            /// bd-3h7: emitted alongside `.error` when Apple rejects a
            /// window with `LanguageModelSession.GenerationError.refusal`.
            /// Captures a `String(reflecting:)` dump of the public
            /// `Refusal` value so we can investigate which internal
            /// `TranscriptRecord` category tripped the classifier.
            case refusalDetail
        }

        enum SmartShrinkOutcome: String, Sendable {
            case success
            case retried
            case abandoned
        }

        let kind: Kind
        let windowIndex: Int          // 1-based
        let totalWindows: Int
        let firstSegmentIndex: Int?
        let lastSegmentIndex: Int?
        let segmentCount: Int
        let promptTokens: Int
        let promptCharLength: Int
        let promptPreview: String     // first 200 chars, newlines escaped to spaces
        let errorDescription: String  // empty for `.submit`
        let errorReflect: String      // empty for `.submit`
        let status: SemanticScanStatus? // nil for `.submit`
        /// bd-34e Fix B v3: smart-shrink retry iteration (1-based) for
        /// `.smartShrinkAttempt` and `.smartShrinkOutcome` events; nil
        /// otherwise.
        let smartShrinkIteration: Int?
        /// bd-34e Fix B v3: terminal outcome for `.smartShrinkOutcome`;
        /// nil otherwise.
        let smartShrinkOutcome: SmartShrinkOutcome?

        init(
            kind: Kind,
            windowIndex: Int,
            totalWindows: Int,
            firstSegmentIndex: Int?,
            lastSegmentIndex: Int?,
            segmentCount: Int,
            promptTokens: Int,
            promptCharLength: Int,
            promptPreview: String,
            errorDescription: String,
            errorReflect: String,
            status: SemanticScanStatus?,
            smartShrinkIteration: Int? = nil,
            smartShrinkOutcome: SmartShrinkOutcome? = nil
        ) {
            self.kind = kind
            self.windowIndex = windowIndex
            self.totalWindows = totalWindows
            self.firstSegmentIndex = firstSegmentIndex
            self.lastSegmentIndex = lastSegmentIndex
            self.segmentCount = segmentCount
            self.promptTokens = promptTokens
            self.promptCharLength = promptCharLength
            self.promptPreview = promptPreview
            self.errorDescription = errorDescription
            self.errorReflect = errorReflect
            self.status = status
            self.smartShrinkIteration = smartShrinkIteration
            self.smartShrinkOutcome = smartShrinkOutcome
        }
    }

    /// Internal test hook so unit tests can observe coarse-pass window submit /
    /// error diagnostics without scraping `os.Logger`. Mirrors
    /// `refinementDecodeFailureObserver`. Production builds leave it nil;
    /// invoking it is a no-op.
    nonisolated(unsafe) static var coarsePassDiagnosticObserver: (@Sendable (CoarsePassWindowDiagnostic) -> Void)?

    /// Static identifier for the refinement @Generable schema. Used by the
    /// diagnostic payload so future schema rotations are visible in logs.
    static let refinementSchemaName = "RefinementWindowSchema"

    private let runtime: Runtime
    private let config: Config
    private let logger: Logger

    init(
        runtime: Runtime? = nil,
        config: Config = .default,
        logger: Logger = Logger(subsystem: "com.playhead", category: "FoundationModelClassifier")
    ) {
        self.config = config
        self.logger = logger
        self.runtime = runtime ?? Self.liveRuntime(logger: logger, config: config)
        // bd-34e: announce the experiment flag at construction so the
        // on-device benchmark log unambiguously confirms whether the
        // PLAYHEAD_FM_DROP_PREAMBLE switch took effect for this run.
        if !Self.injectionPreambleEnabled() {
            logger.debug("PLAYHEAD_FM_DROP_PREAMBLE active — coarse and refinement preambles disabled")
        }
        // bd-34e Hypothesis F: announce the prompt-variant flag at construction
        // so the on-device shadow benchmark log unambiguously confirms which
        // framing the FM saw for this run.
        #if DEBUG
        let variant = Self.coarsePromptVariant()
        switch variant {
        case .classification:
            break
        case .extract:
            logger.debug("PLAYHEAD_FM_PROMPT_VARIANT=extract — using extraction-framed prompt")
        case .neutral:
            logger.debug("PLAYHEAD_FM_PROMPT_VARIANT=neutral — using neutral-question prompt")
        }
        #endif
    }

    func coarsePassA(
        segments: [AdTranscriptSegment],
        locale: Locale = .current
    ) async throws -> FMCoarseScanOutput {
        let clock = ContinuousClock()
        let start = clock.now

        if let status = await runtime.availabilityStatus(locale) {
            return FMCoarseScanOutput(
                status: status,
                windows: [],
                latencyMillis: 0,
                prewarmHit: false
            )
        }

        let plans = try await planPassA(segments: segments)
        guard !plans.isEmpty else {
            return FMCoarseScanOutput(
                status: .success,
                windows: [],
                latencyMillis: 0,
                prewarmHit: false
            )
        }

        let coarseLineRefLookup = lineRefLookup(for: segments)
        // bd-34e Fix B v4: when PLAYHEAD_FM_FRESH_SESSION_PER_WINDOW is set
        // we create a brand-new prewarmed session for every coarse window
        // instead of sharing one across the pass. The default (production)
        // path remains the C6 shared-session optimization.
        let freshSessionPerWindow = Self.freshSessionPerWindowEnabled()
        let sharedBoxOrNil: SessionBox?
        if freshSessionPerWindow {
            logger.debug(
                "PLAYHEAD_FM_FRESH_SESSION_PER_WINDOW active — creating fresh session per coarse window"
            )
            sharedBoxOrNil = nil
        } else {
            // C6: Share a single prewarmed session across all windows in this pass.
            let box = SessionBox(session: await runtime.makeSession())
            await box.prewarm(Self.promptPrefix)
            sharedBoxOrNil = box
        }
        let prewarmHit = true

        var windows: [FMCoarseWindowOutput] = []
        windows.reserveCapacity(plans.count)
        // bd-34e Fix B v3: per-window failures from the smart-shrink retry
        // loop are recorded here so a single oversized window does not
        // abort the entire pass. Other failure types (refusal, guardrail,
        // decoding) still abort to preserve their existing escalation
        // semantics.
        var failedWindowStatuses: [SemanticScanStatus] = []

        let totalWindows = plans.count
        for (planIndex, plan) in plans.enumerated() {
            // H9: Honor cooperative cancellation between windows.
            do {
                try Task.checkCancellation()
            } catch {
                return FMCoarseScanOutput(
                    status: .cancelled,
                    windows: windows,
                    latencyMillis: Self.latencyMillis(since: start, clock: clock),
                    prewarmHit: prewarmHit,
                    failedWindowStatuses: failedWindowStatuses
                )
            }

            let perWindowBox: SessionBox
            if let shared = sharedBoxOrNil {
                perWindowBox = shared
            } else {
                let fresh = SessionBox(session: await runtime.makeSession())
                await fresh.prewarm(Self.promptPrefix)
                perWindowBox = fresh
            }
            switch await coarseResponses(
                for: plan,
                sessionBox: perWindowBox,
                lineRefLookup: coarseLineRefLookup,
                clock: clock,
                windowIndex: planIndex + 1,
                totalWindows: totalWindows
            ) {
            case let .success(outputs):
                for output in outputs {
                    windows.append(
                        FMCoarseWindowOutput(
                            windowIndex: windows.count,
                            lineRefs: output.lineRefs,
                            startTime: output.startTime,
                            endTime: output.endTime,
                            transcriptQuality: output.transcriptQuality,
                            screening: output.screening,
                            latencyMillis: output.latencyMillis
                        )
                    )
                }
            case let .failure(status):
                // bd-34e Fix B v3: a window that exhausts smart-shrink
                // retries (or otherwise fails with `.exceededContextWindow`)
                // is recorded as a per-window failure and the pass
                // continues. This guarantees that one tokenization
                // outlier in a 10+ window pass cannot kill the other
                // 9 windows worth of coarse classification.
                // bd-34e Fix B v3 + bd-3h7: both `.exceededContextWindow`
                // (tokenizer undercount) and `.refusal` (Apple's safety
                // classifier rejecting specific content topics) are
                // per-window conditions. Continuing past them preserves
                // the other windows' coarse coverage instead of aborting
                // the entire pass on the first sensitive window.
                if status == .exceededContextWindow || status == .refusal {
                    failedWindowStatuses.append(status)
                    logger.error(
                        """
                        fm.classifier.coarse_pass_window_abandoned \
                        window=\(planIndex + 1, privacy: .public) \
                        totalWindows=\(totalWindows, privacy: .public) \
                        firstSegmentIndex=\(plan.lineRefs.first ?? -1, privacy: .public) \
                        lastSegmentIndex=\(plan.lineRefs.last ?? -1, privacy: .public) \
                        segmentCount=\(plan.lineRefs.count, privacy: .public) \
                        status=\(status.rawValue, privacy: .public)
                        """
                    )
                    continue
                }
                // C4/H2: Other failures (refusal, guardrail, decoding,
                // cancellation) still abort the pass with partial results.
                return FMCoarseScanOutput(
                    status: status,
                    windows: windows,
                    latencyMillis: Self.latencyMillis(since: start, clock: clock),
                    prewarmHit: prewarmHit,
                    failedWindowStatuses: failedWindowStatuses
                )
            }
        }

        // bd-34e Fix B v3: top-level status remains `.success` whenever at
        // least one window produced output, even if other windows were
        // abandoned by the smart-shrink loop. The runner can inspect
        // `failedWindowStatuses` to surface partial failures in the UI
        // and persist per-window scan rows. If EVERY window failed we
        // surface the last failure status so the caller still sees an
        // unrecoverable error.
        let topLevelStatus: SemanticScanStatus
        if windows.isEmpty, let lastFailure = failedWindowStatuses.last {
            topLevelStatus = lastFailure
        } else {
            topLevelStatus = .success
        }

        return FMCoarseScanOutput(
            status: topLevelStatus,
            windows: windows,
            latencyMillis: Self.latencyMillis(since: start, clock: clock),
            prewarmHit: prewarmHit,
            failedWindowStatuses: failedWindowStatuses
        )
    }

    func planPassA(segments: [AdTranscriptSegment]) async throws -> [CoarsePassWindowPlan] {
        let ordered = segments.sorted { lhs, rhs in
            if lhs.segmentIndex == rhs.segmentIndex {
                return lhs.startTime < rhs.startTime
            }
            return lhs.segmentIndex < rhs.segmentIndex
        }
        guard !ordered.isEmpty else { return [] }

        let budget = try await promptBudget()
        var plans: [CoarsePassWindowPlan] = []
        plans.reserveCapacity(ordered.count)

        var lowerBound = 0
        while lowerBound < ordered.count {
            let firstSegment = ordered[lowerBound]
            var upperBound = lowerBound
            var bestPrompt = Self.buildPrompt(for: [firstSegment])
            var bestTokens = try await runtime.tokenCount(bestPrompt)

            if bestTokens > budget {
                throw FoundationModelClassifierError.segmentExceedsTokenBudget(
                    lineRef: firstSegment.segmentIndex,
                    tokenCount: bestTokens,
                    budget: budget
                )
            }

            var probe = lowerBound + 1
            while probe < ordered.count {
                let candidate = Array(ordered[lowerBound...probe])
                let prompt = Self.buildPrompt(for: candidate)
                let tokenCount = try await runtime.tokenCount(prompt)
                guard tokenCount <= budget else { break }
                upperBound = probe
                bestPrompt = prompt
                bestTokens = tokenCount
                probe += 1
            }

            let windowSegments = Array(ordered[lowerBound...upperBound])
            plans.append(
                CoarsePassWindowPlan(
                    windowIndex: plans.count,
                    lineRefs: windowSegments.map(\.segmentIndex),
                    prompt: bestPrompt,
                    promptTokenCount: bestTokens,
                    startTime: windowSegments.first?.startTime ?? 0,
                    endTime: windowSegments.last?.endTime ?? 0,
                    transcriptQuality: aggregateTranscriptQuality(for: windowSegments)
                )
            )

            lowerBound = upperBound + 1
        }

        return plans
    }

    func planAdaptiveZoom(
        coarse: FMCoarseScanOutput,
        segments: [AdTranscriptSegment],
        evidenceCatalog: EvidenceCatalog
    ) async throws -> [RefinementWindowPlan] {
        guard coarse.status == .success else { return [] }

        let orderedSegments = orderedSegmentsByLineRef(segments)
        guard !orderedSegments.isEmpty else { return [] }

        let budget = try await promptBudget(
            schemaTokens: runtime.refinementSchemaTokenCount,
            maximumResponseTokens: config.refinementMaximumResponseTokens
        )
        let lineRefLookup = lineRefLookup(for: orderedSegments)
        let lineRefByAtomOrdinal = lineRefByAtomOrdinal(for: orderedSegments)
        var plans: [RefinementWindowPlan] = []

        for window in coarse.windows where shouldRefine(window.screening.disposition) {
            let availableLineRefs = window.lineRefs.filter { lineRefLookup[$0] != nil }.sorted()
            guard !availableLineRefs.isEmpty else { continue }

            let focusLineRefs = focusLineRefs(
                for: window,
                availableLineRefs: availableLineRefs
            )
            let focusClusters = buildFocusClusters(from: focusLineRefs)

            var selectedLineRefs = Set<Int>()
            for cluster in focusClusters {
                for lineRef in expandedCluster(cluster, availableLineRefs: availableLineRefs) {
                    selectedLineRefs.insert(lineRef)
                }
            }

            var stopReason: ZoomStopReason = focusClusters.count > 1 ? .ambiguityBudget : .minimumSpan
            var orderedLineRefs = selectedLineRefs.sorted()
            var promptEvidence = promptEvidenceEntries(
                for: orderedLineRefs,
                evidenceCatalog: evidenceCatalog,
                lineRefByAtomOrdinal: lineRefByAtomOrdinal
            )
            var prompt = Self.buildRefinementPrompt(
                for: orderedLineRefs.compactMap { lineRefLookup[$0] },
                promptEvidence: promptEvidence,
                maximumSpans: config.maximumRefinementSpansPerWindow
            )
            var tokenCount = try await runtime.tokenCount(prompt)

            if tokenCount > budget {
                stopReason = .tokenBudget
                orderedLineRefs = focusLineRefs.isEmpty
                    ? orderedLineRefs
                    : Array(Set(focusLineRefs)).sorted()
                promptEvidence = promptEvidenceEntries(
                    for: orderedLineRefs,
                    evidenceCatalog: evidenceCatalog,
                    lineRefByAtomOrdinal: lineRefByAtomOrdinal
                )
                prompt = Self.buildRefinementPrompt(
                    for: orderedLineRefs.compactMap { lineRefLookup[$0] },
                    promptEvidence: promptEvidence,
                    maximumSpans: config.maximumRefinementSpansPerWindow
                )
                tokenCount = try await runtime.tokenCount(prompt)
            }

            guard !orderedLineRefs.isEmpty else { continue }
            if tokenCount > budget, let overflowingLineRef = orderedLineRefs.first {
                throw FoundationModelClassifierError.segmentExceedsTokenBudget(
                    lineRef: overflowingLineRef,
                    tokenCount: tokenCount,
                    budget: budget
                )
            }

            plans.append(
                RefinementWindowPlan(
                    windowIndex: plans.count,
                    sourceWindowIndex: window.windowIndex,
                    lineRefs: orderedLineRefs,
                    focusLineRefs: focusLineRefs,
                    focusClusters: focusClusters,
                    prompt: prompt,
                    promptTokenCount: tokenCount,
                    startTime: orderedLineRefs.compactMap { lineRefLookup[$0]?.startTime }.min() ?? 0,
                    endTime: orderedLineRefs.compactMap { lineRefLookup[$0]?.endTime }.max() ?? 0,
                    stopReason: stopReason,
                    promptEvidence: promptEvidence
                )
            )
        }

        return plans
    }

    func refinePassB(
        zoomPlans: [RefinementWindowPlan],
        segments: [AdTranscriptSegment],
        evidenceCatalog: EvidenceCatalog,
        locale: Locale = .current
    ) async throws -> FMRefinementScanOutput {
        let clock = ContinuousClock()
        let start = clock.now

        if let status = await runtime.availabilityStatus(locale) {
            return FMRefinementScanOutput(
                status: status,
                windows: [],
                latencyMillis: 0,
                prewarmHit: false
            )
        }

        guard !zoomPlans.isEmpty else {
            return FMRefinementScanOutput(
                status: .success,
                windows: [],
                latencyMillis: 0,
                prewarmHit: false
            )
        }

        let lineRefLookup = lineRefLookup(for: segments)
        // bd-34e Fix B v4: same fresh-session-per-window experiment switch
        // as the coarse pass — when PLAYHEAD_FM_FRESH_SESSION_PER_WINDOW is
        // set, every refinement window gets its own freshly minted session.
        let freshSessionPerWindow = Self.freshSessionPerWindowEnabled()
        let sharedBoxOrNil: SessionBox?
        if freshSessionPerWindow {
            logger.debug(
                "PLAYHEAD_FM_FRESH_SESSION_PER_WINDOW active — creating fresh session per refinement window"
            )
            sharedBoxOrNil = nil
        } else {
            // C6: Share a single prewarmed session across all refinement windows.
            let box = SessionBox(session: await runtime.makeSession())
            await box.prewarm(Self.refinementPromptPrefix)
            sharedBoxOrNil = box
        }
        let prewarmHit = true

        var windows: [FMRefinementWindowOutput] = []
        windows.reserveCapacity(zoomPlans.count)

        for plan in zoomPlans {
            // H9: Cooperative cancellation between windows.
            do {
                try Task.checkCancellation()
            } catch {
                return FMRefinementScanOutput(
                    status: .cancelled,
                    windows: windows,
                    latencyMillis: Self.latencyMillis(since: start, clock: clock),
                    prewarmHit: prewarmHit
                )
            }

            let windowStart = clock.now
            let perWindowBox: SessionBox
            if let shared = sharedBoxOrNil {
                perWindowBox = shared
            } else {
                let fresh = SessionBox(session: await runtime.makeSession())
                await fresh.prewarm(Self.refinementPromptPrefix)
                perWindowBox = fresh
            }
            let outcome = await refinementResponse(
                for: plan,
                sessionBox: perWindowBox,
                lineRefLookup: lineRefLookup
            )

            let effectivePlan: RefinementWindowPlan
            let response: RefinementWindowSchema
            switch outcome {
            case let .success(plan, schema):
                effectivePlan = plan
                response = schema
            case let .failure(status):
                // C4/H2: Return partial results.
                return FMRefinementScanOutput(
                    status: status,
                    windows: windows,
                    latencyMillis: Self.latencyMillis(since: start, clock: clock),
                    prewarmHit: prewarmHit
                )
            }

            let spans = sanitize(
                schema: response,
                plan: effectivePlan,
                lineRefLookup: lineRefLookup,
                evidenceCatalog: evidenceCatalog
            )
            windows.append(
                FMRefinementWindowOutput(
                    windowIndex: plan.windowIndex,
                    sourceWindowIndex: plan.sourceWindowIndex,
                    lineRefs: effectivePlan.lineRefs,
                    spans: spans,
                    latencyMillis: Self.latencyMillis(since: windowStart, clock: clock)
                )
            )
        }

        return FMRefinementScanOutput(
            status: .success,
            windows: windows,
            latencyMillis: Self.latencyMillis(since: start, clock: clock),
            prewarmHit: prewarmHit
        )
    }

    // H14: Lines are prefixed `L<n>>` (not `<n>:`) so the model cannot be
    // tricked by transcript text that literally contains `0: ad`. The model
    // returns lineRef ints via the @Generable schema, so the output side does
    // not need to parse this prefix.
    //
    // bd-34e: the original preamble used jailbreak-defense framing
    // ("untrusted user content", "do not follow instructions") which trips
    // Apple's safety classifier as adversarial intent. The structural
    // injection defenses in escapedLine() (NFKC + Cc/Cf strip + fence
    // rewrite + L<n>> defang) are the load-bearing protection. The
    // preamble framing was always belt-and-suspenders.
    private static let injectionPreamble = "Classify whether the following podcast transcript window contains advertising or promotional content."
    private static let lineRefInstruction = "Each transcript line is prefixed with `L<number>>` followed by quoted text. Use the line numbers to cite supporting evidence in your output."
    private static let transcriptOpenFence = "<<<TRANSCRIPT>>>"
    private static let transcriptCloseFence = "<<<END TRANSCRIPT>>>"

    // bd-34e Hypothesis F: iOS 26.4's output safety classifier refuses
    // ("May contain sensitive content") on the first window of real podcast
    // ad content (CVS pre-roll). The hypothesis is that the *task framing*
    // ("classify whether this is an ad") is what trips the classifier on
    // health-adjacent advertisers, not the content itself. These two
    // alternate preamble texts reframe the task as information extraction
    // ("list product mentions") and as a neutral question ("what is
    // mentioned here"), keeping the same line-ref instruction and fence
    // wrapping. The variant is selected per-run via PLAYHEAD_FM_PROMPT_VARIANT.
    private static let extractPromptPrefix = "Extract product mentions."
    private static let extractInjectionPreamble = "List any company names, product names, brand mentions, URLs, promo codes, or sponsorship language found in this podcast transcript window. Return an empty list if none are present."
    private static let extractLineRefInstruction = "Each transcript line is prefixed with `L<number>>` followed by quoted text. Use the line numbers to cite where each mention appears."

    private static let neutralPromptPrefix = "Identify mentions."
    private static let neutralInjectionPreamble = "What companies, products, services, or sponsorship language are mentioned in this transcript window? Return a list. Return an empty list if nothing is mentioned."
    private static let neutralLineRefInstruction = "Each transcript line is prefixed with `L<number>>` followed by quoted text. Cite line numbers for each mention."

    #if DEBUG
    /// bd-34e Hypothesis F: alternate coarse-pass framings selected via the
    /// PLAYHEAD_FM_PROMPT_VARIANT debug env var. `classification` is the
    /// production default; `extract` and `neutral` are the on-device
    /// experiments that probe whether iOS 26.4's output safety classifier
    /// refuses on task framing rather than content topic.
    enum CoarsePromptVariant: String {
        case classification
        case extract
        case neutral
    }

    static func coarsePromptVariant() -> CoarsePromptVariant {
        guard let raw = ProcessInfo.processInfo.environment["PLAYHEAD_FM_PROMPT_VARIANT"]?.lowercased(),
              !raw.isEmpty else {
            return .classification
        }
        return CoarsePromptVariant(rawValue: raw) ?? .classification
    }
    #else
    enum CoarsePromptVariant: String {
        case classification
    }
    static func coarsePromptVariant() -> CoarsePromptVariant { .classification }
    #endif

    /// bd-34e: when this returns false, the H14 injection preamble (the
    /// "untrusted user content" line, the line-ref instruction, and the
    /// `<<<TRANSCRIPT>>>` fences) is dropped from every coarse and refinement
    /// prompt. This is the controlled experiment switch the on-device shadow
    /// benchmark uses to test whether Apple's safety classifier trips on the
    /// preamble framing for long ad-heavy windows. The flag is debug-only —
    /// release builds always return `true`.
    static func injectionPreambleEnabled() -> Bool {
        #if DEBUG
        if ProcessInfo.processInfo.environment["PLAYHEAD_FM_DROP_PREAMBLE"] != nil {
            return false
        }
        #endif
        return true
    }

    /// bd-34e Fix B v4: when this returns true, every coarse and refinement
    /// window gets its own freshly minted `LanguageModelSession` (via
    /// `runtime.makeSession()`) instead of sharing one across the pass.
    /// This is the controlled experiment switch the on-device shadow
    /// benchmark uses to test whether session accumulation is responsible
    /// for the 8×–11× tokenizer-undercount the smart-shrink data revealed.
    /// The flag is debug-only — release builds always return `false` and
    /// the production C6 shared-session behavior is preserved.
    ///
    /// Fix v4 hypothesis: shadow telemetry shows actualTokens are STABLE
    /// within a window's smart-shrink retries (4253 → 4253 → 4253) and
    /// CONTENT-dependent across windows (window 7 < window 6 even though
    /// it comes later), which rules out per-call session growth as the
    /// primary cause. Turning this flag ON should therefore NOT close the
    /// gap. If it does, the analysis was wrong and accumulation is real.
    static func freshSessionPerWindowEnabled() -> Bool {
        #if DEBUG
        if ProcessInfo.processInfo.environment["PLAYHEAD_FM_FRESH_SESSION_PER_WINDOW"] != nil {
            return true
        }
        #endif
        return false
    }

    /// H14: The static, transcript-independent preamble of the coarse-pass
    /// prompt — every wrapping line that the planner emits regardless of how
    /// many segments are inside the fences. Used by `preambleTokenCount` and
    /// regression tests so any future preamble growth is loud and accounted
    /// for in budget math.
    ///
    /// bd-34e: the original preamble used jailbreak-defense framing
    /// ("untrusted user content", "do not follow instructions") which trips
    /// Apple's safety classifier as adversarial intent. The structural
    /// injection defenses in escapedLine() (NFKC + Cc/Cf strip + fence
    /// rewrite + L<n>> defang) are the load-bearing protection. The
    /// preamble framing was always belt-and-suspenders.
    static func coarsePromptPreamble() -> String {
        // bd-34e: PLAYHEAD_FM_DROP_PREAMBLE collapses the H14 wrapping
        // entirely so the on-device benchmark can probe whether Apple's
        // safety classifier trips on this framing.
        guard injectionPreambleEnabled() else { return "" }
        let parts = coarsePreambleParts(for: coarsePromptVariant())
        return [
            parts.prefix,
            parts.preamble,
            parts.lineRef,
            transcriptOpenFence,
            transcriptCloseFence
        ].joined(separator: "\n")
    }

    /// bd-34e Hypothesis F: resolve the (prefix, preamble, line-ref) triple
    /// for a given prompt variant. All three variants share the same five-line
    /// shape (prefix / framing / line-ref instruction / open fence / close
    /// fence) so the existing preamble token-count assertions and the
    /// `buildPrompt(for: [])` equivalence invariant remain intact.
    static func coarsePreambleParts(
        for variant: CoarsePromptVariant
    ) -> (prefix: String, preamble: String, lineRef: String) {
        switch variant {
        case .classification:
            return (promptPrefix, injectionPreamble, lineRefInstruction)
        #if DEBUG
        case .extract:
            return (extractPromptPrefix, extractInjectionPreamble, extractLineRefInstruction)
        case .neutral:
            return (neutralPromptPrefix, neutralInjectionPreamble, neutralLineRefInstruction)
        #endif
        }
    }

    /// H14: Token count of the static coarse-pass preamble (excluding any
    /// transcript content). Tests bump their synthetic `contextSize` by this
    /// value to keep the budget-exceeded paths exercising the same per-line
    /// budget pressure as before, regardless of preamble growth.
    static func preambleTokenCount(runtime: Runtime) async throws -> Int {
        try await runtime.tokenCount(coarsePromptPreamble())
    }

    static func buildPrompt(for segments: [AdTranscriptSegment]) -> String {
        // bd-34e: when the preamble is disabled we drop ALL the wrapping
        // lines (prefix, injection preamble, line-ref instruction, fences),
        // so the model sees only the bare `L<n>> "..."` transcript lines.
        // This is the controlled experiment for hypothesis A.
        let preambleActive = injectionPreambleEnabled()
        var lines: [String] = []
        if preambleActive {
            let parts = coarsePreambleParts(for: coarsePromptVariant())
            lines.append(contentsOf: [
                parts.prefix,
                parts.preamble,
                parts.lineRef,
                transcriptOpenFence
            ])
        }
        lines.append(contentsOf: segments.map { segment in
            "L\(segment.segmentIndex)> \"\(escapedLine(segment.text))\""
        })
        if preambleActive {
            lines.append(transcriptCloseFence)
        }
        return lines.joined(separator: "\n")
    }

    private static func buildRefinementPrompt(
        for segments: [AdTranscriptSegment],
        promptEvidence: [PromptEvidenceEntry],
        maximumSpans: Int
    ) -> String {
        // bd-34e: same flag as `buildPrompt(for:)` — drop the H14 framing
        // (prefix, injection preamble, line-ref instruction, fences) when the
        // experiment switch is set.
        let preambleActive = injectionPreambleEnabled()
        var lines: [String] = []
        if preambleActive {
            lines.append(contentsOf: [
                refinementPromptPrefix,
                injectionPreamble,
                lineRefInstruction,
                "Transcript:",
                transcriptOpenFence
            ])
        }
        lines.append(contentsOf: segments.map { segment in
            "L\(segment.segmentIndex)> \"\(escapedLine(segment.text))\""
        })
        if preambleActive {
            lines.append(transcriptCloseFence)
        }
        if !promptEvidence.isEmpty {
            lines.append("Evidence catalog:")
            lines.append(contentsOf: promptEvidence.map { $0.renderForPrompt() })
        }
        lines.append("Return up to \(maximumSpans) spans.")
        return lines.joined(separator: "\n")
    }

    private func promptBudget() async throws -> Int {
        try await promptBudget(
            schemaTokens: runtime.coarseSchemaTokenCount,
            maximumResponseTokens: config.coarseMaximumResponseTokens,
            divisor: Self.coarseBudgetDivisor
        )
    }

    private func promptBudget(
        schemaTokens: @escaping @Sendable () async throws -> Int,
        maximumResponseTokens: Int,
        divisor: Int = Self.refinementBudgetDivisor
    ) async throws -> Int {
        let contextSize = await runtime.contextSize()
        let schemaTokenCount = try await schemaTokens()
        return Self.maximumEstimatedPromptTokensSafeFor(
            contextSize: contextSize,
            schemaTokens: schemaTokenCount,
            maximumResponseTokens: maximumResponseTokens,
            safetyMarginTokens: config.safetyMarginTokens,
            divisor: divisor
        )
    }

    /// bd-34e Fix B v1 (refinement, unchanged): the structured
    /// `L<n>> "..."` line-ref prompt format tokenizes ~2.1× worse than
    /// the planner's estimator predicts. The refinement path keeps the
    /// halving compromise it shipped with — its prompts are smaller and
    /// the refinement decode-failure work (bd-3h2) is being investigated
    /// independently, so we deliberately do not retune it here.
    static let refinementBudgetDivisor = 2

    /// bd-34e Fix B v4 (coarse): Fix v3's ÷4 divisor still left every
    /// Conan-episode coarse window 8.3×–10.8× over Apple's actual budget
    /// on real device. Real-device shadow telemetry from Phase 3 shows
    /// the actual tokenizer ratio is much worse than the 3.45× the v3
    /// round was sized for. Most likely cause: Apple's
    /// `LanguageModelSession.tokenCount(for:)` only counts the prompt
    /// content, but the on-call accounting also includes the @Generable
    /// schema's serialized form (CoarseScreeningSchema doc strings,
    /// nested types, enum cases) plus an opaque session-state preamble.
    /// We move from ÷4 to ÷8 to give the coarse prompt an 8× safety
    /// factor. This produces ~20–25 small windows per episode (slow but
    /// safe) and the smart-shrink retry loop (see `coarseResponses`)
    /// catches the remaining outliers without aborting the whole pass.
    static let coarseBudgetDivisor = 8

    /// bd-34e Fix B v3: maximum number of smart-shrink retry iterations
    /// before a single coarse window is abandoned. Each iteration
    /// re-derives target segment count from the most-recent Apple-reported
    /// actual token count, so three iterations are usually enough to land
    /// inside Apple's hidden ceiling even when the per-segment cost varies
    /// by 2–3× across attempts.
    static let coarseSmartShrinkMaxIterations = 3

    /// Compute the safe ceiling for estimator-counted prompt tokens. The
    /// `divisor` is the safety multiple against tokenizer undercount and
    /// also clamps the ceiling via `contextSize / divisor` so that no
    /// single window ever budgets beyond `1/divisor` of context, even
    /// under pathological zero-overhead inputs.
    static func maximumEstimatedPromptTokensSafeFor(
        contextSize: Int,
        schemaTokens: Int,
        maximumResponseTokens: Int,
        safetyMarginTokens: Int,
        divisor: Int = refinementBudgetDivisor
    ) -> Int {
        let safeDivisor = max(1, divisor)
        let preMargin = contextSize - schemaTokens - maximumResponseTokens - safetyMarginTokens
        let conservative = preMargin / safeDivisor
        let hardCap = contextSize / safeDivisor
        return max(1, min(conservative, hardCap))
    }

    /// bd-34e Fix B v2: parse Apple's reported actual token count out of
    /// a `LanguageModelSession.GenerationError.exceededContextWindowSize`
    /// error so the smart-shrink retry knows exactly how oversized a
    /// window was. Apple's error structure is undocumented and the count
    /// shows up in different places across iOS releases (the localized
    /// description, the `Context.debugDescription`, the underlying
    /// errors, the `userInfo` strings), so we cast a wide net and pick
    /// the first numeric run that looks like a token count.
    ///
    /// Examples we have observed in the wild:
    ///   "Content contains 4295 tokens, which exceeds the maximum allowed context size of 4096."
    ///   "Provided 4,295 tokens, but the maximum allowed is 4,096."
    static func extractActualTokenCount(from error: Error) -> Int? {
        var candidates: [String] = [
            error.localizedDescription,
            String(reflecting: error)
        ]
        let nsError = error as NSError
        candidates.append(contentsOf: nsError.userInfo.values.compactMap { $0 as? String })
        for underlying in nsError.underlyingErrors {
            candidates.append(underlying.localizedDescription)
            candidates.append(String(reflecting: underlying))
        }

        guard let pattern = try? NSRegularExpression(pattern: #"(\d[\d,]*)\s*tokens"#) else {
            return nil
        }
        for candidate in candidates {
            let nsRange = NSRange(candidate.startIndex..., in: candidate)
            if let match = pattern.firstMatch(in: candidate, range: nsRange),
               let numberRange = Range(match.range(at: 1), in: candidate) {
                let cleaned = candidate[numberRange].replacingOccurrences(of: ",", with: "")
                if let value = Int(cleaned) {
                    return value
                }
            }
        }
        return nil
    }

    private func aggregateTranscriptQuality(for segments: [AdTranscriptSegment]) -> TranscriptQuality {
        let qualities = TranscriptQualityEstimator.assess(segments: segments).map(\.quality)
        if qualities.contains(.unusable) {
            return .unusable
        }
        if qualities.contains(.degraded) {
            return .degraded
        }
        return .good
    }

    /// M25: Maximum number of supportLineRefs the coarse sanitize path will
    /// retain per window. The model can (or, under FM compression, will)
    /// occasionally emit dozens of refs; everything beyond this cap is
    /// dropped with a log so downstream zoom planning sees a bounded set.
    static let maximumCoarseSupportLineRefs = 32

    private func sanitize(
        schema: CoarseScreeningSchema,
        validLineRefs: Set<Int>
    ) -> CoarseScreeningSchema {
        let sanitizedSupport: CoarseSupportSchema?
        switch schema.disposition {
        case .noAds, .abstain:
            sanitizedSupport = nil
        case .containsAd, .uncertain:
            if let support = schema.support {
                // M-R3-2: Cap BEFORE dedup so an attacker/FM cannot flood
                // the list with duplicates that dedupe to exactly 32 and
                // hide the legitimate top-32 evidence behind a fabricated
                // prefix. The cap must bound INGESTED data, not the
                // post-dedup tail.
                //
                // We also pre-filter to valid line refs before the cap, so
                // junk refs cannot burn the 32-slot budget. The dedup pass
                // then runs on the capped prefix and may legitimately
                // produce fewer than 32 survivors if duplicates exist.
                let validInOrder = support.supportLineRefs.filter { validLineRefs.contains($0) }
                let rawIncoming = validInOrder.count
                let capped = Array(validInOrder.prefix(Self.maximumCoarseSupportLineRefs))
                var deduped: [Int] = []
                var seen: Set<Int> = []
                for lineRef in capped where seen.insert(lineRef).inserted {
                    deduped.append(lineRef)
                }

                let droppedCount: Int
                if rawIncoming > Self.maximumCoarseSupportLineRefs {
                    droppedCount = rawIncoming - Self.maximumCoarseSupportLineRefs
                } else {
                    droppedCount = 0
                }
                if droppedCount > 0 {
                    logger.warning(
                        "fm.classifier.support_line_refs_capped dropped=\(droppedCount, privacy: .public) cap=\(Self.maximumCoarseSupportLineRefs, privacy: .public)"
                    )
                }

                if deduped.isEmpty {
                    sanitizedSupport = nil
                } else {
                    sanitizedSupport = CoarseSupportSchema(
                        supportLineRefs: deduped,
                        certainty: support.certainty
                    )
                }
            } else {
                sanitizedSupport = nil
            }
        }

        return CoarseScreeningSchema(
            disposition: schema.disposition,
            support: sanitizedSupport
        )
    }

    private func sanitize(
        schema: RefinementWindowSchema,
        plan: RefinementWindowPlan,
        lineRefLookup: [Int: AdTranscriptSegment],
        evidenceCatalog: EvidenceCatalog
    ) -> [RefinedAdSpan] {
        var spans: [RefinedAdSpan] = []
        let validLineRefs = Set(plan.lineRefs)

        // M20: Log silent span truncation when the model returns more spans
        // than `maximumRefinementSpansPerWindow`. This helps surface a real
        // signal vs. silently dropping data.
        if schema.spans.count > config.maximumRefinementSpansPerWindow {
            logger.warning(
                "fm.classifier.span_truncation incoming=\(schema.spans.count, privacy: .public) cap=\(self.config.maximumRefinementSpansPerWindow, privacy: .public)"
            )
        }

        // M24: `evidenceRef` on `EvidenceAnchorSchema` is the catalog-global
        // STABLE ID assigned by `EvidenceCatalogBuilder.assignRefs`, not a
        // positional index into `plan.promptEvidence`. Build a set of the
        // stable ids actually presented to the model so we can reject
        // fabricated refs without confusing them with valid ids that happen
        // to fall outside `0..<promptEvidence.count`.
        let presentedEvidenceRefs: Set<Int> = Set(plan.promptEvidence.map { $0.entry.evidenceRef })
        // H15: Cap evidence anchors per span before passing to the resolver.
        let anchorCap = max(1, config.maximumRefinementSpansPerWindow * 8)

        for span in schema.spans.prefix(config.maximumRefinementSpansPerWindow) {
            let firstLineRef = min(span.firstLineRef, span.lastLineRef)
            let lastLineRef = max(span.firstLineRef, span.lastLineRef)
            guard validLineRefs.contains(firstLineRef),
                  validLineRefs.contains(lastLineRef),
                  (firstLineRef...lastLineRef).allSatisfy({ validLineRefs.contains($0) }),
                  let firstSegment = lineRefLookup[firstLineRef],
                  let lastSegment = lineRefLookup[lastLineRef] else {
                continue
            }

            // M15/M26: Bound refinement span breadth vs. supporting evidence
            // width. Reject anchorless spans outright — without any FM-cited
            // anchor we have nothing to anchor a skip cut to. For non-empty
            // anchor sets, allow at most `count * 4` lines of breadth.
            //
            // The previous floor of `max(8, count * 4)` let an 8-line span
            // through with zero or one anchor, defeating the purpose of the
            // breadth check.
            if span.evidenceAnchors.isEmpty {
                logger.warning(
                    "fm.classifier.span_breadth_rejected reason=anchorless breadth=\(lastLineRef - firstLineRef, privacy: .public)"
                )
                continue
            }
            // H-R3-2: Dedupe anchors by (kind, lineRef, evidenceRef) BEFORE
            // sizing the breadth cap. Without this, an FM could submit four
            // identical duplicate anchors and stretch an anchorless-in-effect
            // 17-line span through a `count * 4` bound. The real resolver
            // collapses these duplicates anyway, so the breadth budget must
            // match what we'll actually retain.
            // R4-Fix3: dedupe on (kind, lineRef) only. The breadth cap
            // represents how many distinct positions in the transcript
            // attest the span; an FM citing four catalog rows at a single
            // lineRef is still ONE position. Including evidenceRef in the
            // key let an FM inflate the cap from 4 lines to 16 by citing
            // distinct catalog ids at the same atom.
            let uniqueAnchorKeys = Set(span.evidenceAnchors.map { anchor in
                AnchorDedupKey(
                    kind: anchor.kind,
                    lineRef: anchor.lineRef
                )
            })
            let maxBreadth = uniqueAnchorKeys.count * 4
            if (lastLineRef - firstLineRef) > maxBreadth {
                logger.warning(
                    "fm.classifier.span_breadth_rejected breadth=\(lastLineRef - firstLineRef, privacy: .public) max=\(maxBreadth, privacy: .public)"
                )
                continue
            }

            // H15: Cap evidence anchor count.
            let cappedAnchors: [EvidenceAnchorSchema]
            if span.evidenceAnchors.count > anchorCap {
                logger.warning(
                    "fm.classifier.anchor_cap_truncation incoming=\(span.evidenceAnchors.count, privacy: .public) cap=\(anchorCap, privacy: .public)"
                )
                cappedAnchors = Array(span.evidenceAnchors.prefix(anchorCap))
            } else {
                cappedAnchors = span.evidenceAnchors
            }

            // M24: Drop anchors whose evidenceRef points outside the prompt's
            // evidence catalog. Keep nil-evidenceRef anchors (those resolve via
            // lineRefFallback in the resolver). Match by stable id, NOT by
            // positional index into `plan.promptEvidence` — see the comment
            // on `presentedEvidenceRefs` above.
            let validatedAnchors = cappedAnchors.filter { anchor in
                guard let ref = anchor.evidenceRef else { return true }
                return presentedEvidenceRefs.contains(ref)
            }

            let rawResolvedEvidenceAnchors = CommercialEvidenceResolver.resolve(
                anchors: validatedAnchors,
                plan: plan,
                lineRefLookup: lineRefLookup,
                evidenceCatalog: evidenceCatalog
            )

            // H-R3-1 / R4-Fix5: Enforce span-range containment on ALL
            // resolved anchors regardless of resolution source. The FM can
            // hallucinate a span at lines 1...5 while citing an
            // evidenceRef whose true lineRef is 11 (the resolver maps
            // back to the catalog entry's ORIGINAL lineRef), OR a fallback
            // anchor at lineRef 11 that lives in the window but outside
            // the claimed span. Both are span-range violations and must
            // be dropped — the prior `.evidenceRef`-only gate let
            // fallback anchors slip through and stay attached to the
            // span, defeating the in-memory protection.
            let resolvedEvidenceAnchors = rawResolvedEvidenceAnchors.filter { anchor in
                anchor.lineRef >= firstLineRef && anchor.lineRef <= lastLineRef
            }
            if resolvedEvidenceAnchors.count < rawResolvedEvidenceAnchors.count {
                logger.warning(
                    "fm.classifier.out_of_range_evidence_ref_dropped dropped=\(rawResolvedEvidenceAnchors.count - resolvedEvidenceAnchors.count, privacy: .public) span=\(firstLineRef, privacy: .public)-\(lastLineRef, privacy: .public)"
                )
            }

            // M21: `memoryWriteEligible` uses a CONJUNCTIVE policy: a single
            // unresolved anchor voids the entire span. This is intentional —
            // we only persist memory writes when EVERY supporting anchor was
            // resolved to a deterministic catalog entry.
            //
            // bd-34e schema trim: `alternativeExplanation` and `reasonTags`
            // are no longer part of the slim `@Generable SpanRefinementSchema`
            // (they cost ~13 enum cases of fixed schema overhead per call
            // and the runner does not persist them). They are defaulted to
            // `.unknown` / `[]` here so downstream consumers that read these
            // fields keep working. If a future banner pass needs to surface
            // them again, derive them deterministically from `commercialIntent`
            // and the resolved evidence kinds rather than asking the FM.
            spans.append(
                RefinedAdSpan(
                    commercialIntent: span.commercialIntent,
                    ownership: span.ownership,
                    firstLineRef: firstLineRef,
                    lastLineRef: lastLineRef,
                    firstAtomOrdinal: firstSegment.firstAtomOrdinal,
                    lastAtomOrdinal: lastSegment.lastAtomOrdinal,
                    certainty: span.certainty,
                    boundaryPrecision: span.boundaryPrecision,
                    resolvedEvidenceAnchors: resolvedEvidenceAnchors,
                    memoryWriteEligible: !resolvedEvidenceAnchors.isEmpty &&
                        resolvedEvidenceAnchors.allSatisfy(\.memoryWriteEligible),
                    alternativeExplanation: .unknown,
                    reasonTags: []
                )
            )
        }

        return spans
    }

    /// H-R3-2 / R4-Fix3: Dedup key for evidence anchors. The key represents
    /// a distinct position in the transcript: two anchors at the same
    /// `(kind, lineRef)` collapse to one regardless of which catalog row
    /// the FM cited. Including `evidenceRef` in the key let an FM cite
    /// four catalog ids at a single atom and inflate the breadth cap by
    /// `count * 4` from a single line.
    private struct AnchorDedupKey: Hashable {
        let kind: EvidenceAnchorKind
        let lineRef: Int
    }

    /// Dedupes `reasonTags` and drops tags that are semantically inconsistent
    /// with the span's `commercialIntent`. Organic spans cannot carry
    /// commerce-asserting tags (promo codes, CTAs, URL callouts, brand/host-
    /// read pitches, cross-promo language). Paid/owned/affiliate spans accept
    /// any tag. Unknown intent is treated conservatively the same as other
    /// commercial variants (no filtering).
    ///
    /// **`.guestPlug` AND `.disclosure` are intentionally allowed on organic
    /// spans.** This is not an oversight: the project's ad-content gradient
    /// (see the project memory note `project_ad_gradient.md`) treats borderline
    /// content as banner-eligible. The `commercialIntent` field drives the
    /// auto-skip decision, and the `reasonTags` field drives the banner hint
    /// shown to the user. Two symmetric carve-outs apply:
    ///
    ///   * `.guestPlug` — stripping it from organic spans would erase the
    ///     banner cue for guest self-promotion (e.g. "go pre-order my book").
    ///     Guests plugging their own work without commerce structure stays
    ///     organic on intent but still surfaces in the banner via this tag.
    ///   * `.disclosure` — a host saying "this isn't a paid promotion, but..."
    ///     is editorial content that contains an FCC-style disclosure phrase.
    ///     Stripping the tag loses the banner-display signal even though the
    ///     intent is correctly classified as organic. Symmetric with
    ///     `.guestPlug`: both are borderline signals that deserve banner
    ///     display without flipping classification.
    ///
    /// Produces a sorted, unique array without allocating an intermediate
    /// Set — the tag list is short, so a sort + manual dedup is cheaper than
    /// Set materialization and pays off for every refinement span.
    static func sanitizeReasonTags(
        _ tags: [ReasonTag],
        commercialIntent: CommercialIntent,
        logger: Logger
    ) -> [ReasonTag] {
        guard !tags.isEmpty else { return [] }

        // Sort first (small N, rawValue-based stable ordering), then dedup
        // in place with a single pass — no Set allocation.
        let sorted = tags.sorted { $0.rawValue < $1.rawValue }
        var deduped: [ReasonTag] = []
        deduped.reserveCapacity(sorted.count)
        for tag in sorted where deduped.last != tag {
            deduped.append(tag)
        }

        guard commercialIntent == .organic else {
            return deduped
        }

        // Organic content should not carry tags that assert commerce.
        // `.guestPlug` and `.disclosure` are intentionally NOT in this set —
        // see the doc comment above for the ad-gradient rationale.
        let forbidden: Set<ReasonTag> = [
            .promoCode,
            .callToAction,
            .urlMention,
            .brandMention,
            .hostReadPitch,
            .crossPromoLanguage
        ]

        var filtered: [ReasonTag] = []
        filtered.reserveCapacity(deduped.count)
        var droppedCount = 0
        for tag in deduped {
            if forbidden.contains(tag) {
                droppedCount += 1
            } else {
                filtered.append(tag)
            }
        }

        if droppedCount > 0 {
            logger.debug(
                "Dropped \(droppedCount, privacy: .public) reasonTag(s) inconsistent with organic commercialIntent"
            )
        }
        return filtered
    }

    private func coarseResponses(
        for plan: CoarsePassWindowPlan,
        sessionBox: SessionBox,
        lineRefLookup: [Int: AdTranscriptSegment],
        clock: ContinuousClock,
        windowIndex: Int,
        totalWindows: Int
    ) async -> CoarseResponseOutcome {
        let windowStart = clock.now

        // bd-34e: log a structured submit breadcrumb before each
        // `respond` call so we can correlate guardrail blocks with the
        // exact window context that triggered them.
        reportCoarsePassWindowSubmitIfNeeded(
            plan: plan,
            windowIndex: windowIndex,
            totalWindows: totalWindows
        )

        // C4/H2/H9: Catch per-window errors, map via SemanticScanStatus, and
        // honor the documented retry policy.
        do {
            let response = try await sessionBox.respondCoarse(plan.prompt)
            let screening = sanitize(
                schema: response,
                validLineRefs: Set(plan.lineRefs)
            )
            return .success([
                FMCoarseWindowOutput(
                    windowIndex: 0,
                    lineRefs: plan.lineRefs,
                    startTime: plan.startTime,
                    endTime: plan.endTime,
                    transcriptQuality: plan.transcriptQuality,
                    screening: screening,
                    latencyMillis: Self.latencyMillis(since: windowStart, clock: clock)
                )
            ])
        } catch {
            let status = SemanticScanStatus.from(error: error)
            reportCoarsePassErrorIfNeeded(
                plan: plan,
                error: error,
                status: status,
                windowIndex: windowIndex,
                totalWindows: totalWindows
            )
            // bd-3h7: when Apple's safety classifier rejects this window,
            // emit a second breadcrumb that captures the `Refusal` value
            // reflection in full. The public `Refusal` surface only
            // exposes `transcriptEntries`, `explanation`, and
            // `explanationStream`, but `String(reflecting:)` dumps the
            // internal `TranscriptRecord` that Apple's error message
            // references ("Refusal(record: Refusal.TranscriptRecord)").
            // Having that in the log lets us investigate WHY specific
            // podcast windows are refused without reshipping a diagnostic
            // build.
            reportCoarsePassRefusalDetailIfNeeded(
                plan: plan,
                error: error,
                windowIndex: windowIndex,
                totalWindows: totalWindows
            )
            switch status.retryPolicy {
            case .shrinkWindowAndRetryOnce:
                // bd-34e Fix B v3: when Apple reports the actual token
                // count in the error string, use it to compute a
                // smart-shrunken plan that drops just enough segments to
                // fit a *halved* context window. The smart-shrink loop
                // now iterates up to `coarseSmartShrinkMaxIterations`
                // times, recomputing the target segment count from each
                // attempt's actual token count. If extraction fails
                // (older iOS, error string changed, test scaffolding),
                // we fall back to the legacy recursive midpoint split.
                if status == .exceededContextWindow {
                    let outcome = await runCoarseSmartShrinkLoop(
                        initialPlan: plan,
                        initialError: error,
                        lineRefLookup: lineRefLookup,
                        sessionBox: sessionBox,
                        clock: clock,
                        windowIndex: windowIndex,
                        totalWindows: totalWindows
                    )
                    switch outcome {
                    case .succeeded(let result):
                        return result
                    case .abandoned:
                        return .failure(.exceededContextWindow)
                    case .extractionFailed:
                        break // fall through to legacy splitter
                    }
                }
                guard let retryPlans = await shrunkenCoarsePlansForRetry(
                    from: plan,
                    lineRefLookup: lineRefLookup
                ) else {
                    return .failure(status)
                }
                return await runCoarseRetry(
                    retryPlans: retryPlans,
                    sessionBox: sessionBox,
                    clock: clock
                )
            case .backoffAndRetry:
                // Single backoff retry on the same plan.
                try? await Task.sleep(nanoseconds: 50_000_000)
                do {
                    let response = try await sessionBox.respondCoarse(plan.prompt)
                    let screening = sanitize(
                        schema: response,
                        validLineRefs: Set(plan.lineRefs)
                    )
                    return .success([
                        FMCoarseWindowOutput(
                            windowIndex: 0,
                            lineRefs: plan.lineRefs,
                            startTime: plan.startTime,
                            endTime: plan.endTime,
                            transcriptQuality: plan.transcriptQuality,
                            screening: screening,
                            latencyMillis: Self.latencyMillis(since: windowStart, clock: clock)
                        )
                    ])
                } catch {
                    return .failure(SemanticScanStatus.from(error: error))
                }
            default:
                return .failure(status)
            }
        }
    }

    private func runCoarseRetry(
        retryPlans: [CoarsePassWindowPlan],
        sessionBox: SessionBox,
        clock: ContinuousClock
    ) async -> CoarseResponseOutcome {
        var retryOutputs: [FMCoarseWindowOutput] = []
        retryOutputs.reserveCapacity(retryPlans.count)

        for retryPlan in retryPlans {
            let retryStart = clock.now
            do {
                let response = try await sessionBox.respondCoarse(retryPlan.prompt)
                let screening = sanitize(
                    schema: response,
                    validLineRefs: Set(retryPlan.lineRefs)
                )
                retryOutputs.append(
                    FMCoarseWindowOutput(
                        windowIndex: 0,
                        lineRefs: retryPlan.lineRefs,
                        startTime: retryPlan.startTime,
                        endTime: retryPlan.endTime,
                        transcriptQuality: retryPlan.transcriptQuality,
                        screening: screening,
                        latencyMillis: Self.latencyMillis(since: retryStart, clock: clock)
                    )
                )
            } catch {
                return .failure(SemanticScanStatus.from(error: error))
            }
        }

        return .success(retryOutputs)
    }

    /// bd-34e Fix B v3: outcome of the smart-shrink retry loop. Allows
    /// the caller to distinguish "we extracted nothing parseable, fall
    /// back to the legacy splitter" from "we tried our best and the
    /// window is genuinely too big — abandon it".
    private enum SmartShrinkLoopOutcome {
        case succeeded(CoarseResponseOutcome)
        case abandoned
        case extractionFailed
    }

    /// bd-34e Fix B v3: iterative smart-shrink retry loop. Each iteration
    /// re-derives `targetSegments` from the most recent
    /// `exceededContextWindowSize` error's actual token count, then
    /// retries with a smaller plan. Up to
    /// `coarseSmartShrinkMaxIterations` iterations are attempted before
    /// the window is abandoned.
    private func runCoarseSmartShrinkLoop(
        initialPlan: CoarsePassWindowPlan,
        initialError: Error,
        lineRefLookup: [Int: AdTranscriptSegment],
        sessionBox: SessionBox,
        clock: ContinuousClock,
        windowIndex: Int,
        totalWindows: Int
    ) async -> SmartShrinkLoopOutcome {
        var currentPlan = initialPlan
        var currentError: Error = initialError
        var extractedAtLeastOnce = false

        for iteration in 1...Self.coarseSmartShrinkMaxIterations {
            guard let smartPlan = await smartShrunkenCoarsePlan(
                from: currentPlan,
                lineRefLookup: lineRefLookup,
                error: currentError,
                windowIndex: windowIndex,
                totalWindows: totalWindows,
                iteration: iteration
            ) else {
                // Could not extract a token count from the error or the
                // math says no shrink is possible. If this is the very
                // first iteration we hand off to the legacy splitter;
                // otherwise we have made progress already and just stop.
                if !extractedAtLeastOnce {
                    return .extractionFailed
                }
                reportCoarseSmartShrinkOutcome(
                    plan: currentPlan,
                    iteration: iteration,
                    outcome: .abandoned,
                    windowIndex: windowIndex,
                    totalWindows: totalWindows
                )
                return .abandoned
            }
            extractedAtLeastOnce = true

            let retryStart = clock.now
            do {
                let response = try await sessionBox.respondCoarse(smartPlan.prompt)
                let screening = sanitize(
                    schema: response,
                    validLineRefs: Set(smartPlan.lineRefs)
                )
                reportCoarseSmartShrinkOutcome(
                    plan: smartPlan,
                    iteration: iteration,
                    outcome: .success,
                    windowIndex: windowIndex,
                    totalWindows: totalWindows
                )
                return .succeeded(.success([
                    FMCoarseWindowOutput(
                        windowIndex: 0,
                        lineRefs: smartPlan.lineRefs,
                        startTime: smartPlan.startTime,
                        endTime: smartPlan.endTime,
                        transcriptQuality: smartPlan.transcriptQuality,
                        screening: screening,
                        latencyMillis: Self.latencyMillis(since: retryStart, clock: clock)
                    )
                ]))
            } catch {
                let retryStatus = SemanticScanStatus.from(error: error)
                if retryStatus == .exceededContextWindow {
                    reportCoarseSmartShrinkOutcome(
                        plan: smartPlan,
                        iteration: iteration,
                        outcome: .retried,
                        windowIndex: windowIndex,
                        totalWindows: totalWindows
                    )
                    currentPlan = smartPlan
                    currentError = error
                    continue
                }
                // A non-context-window error during a retry — bail out
                // and let the catch arm in `coarseResponses` decide how
                // to surface it. We treat this as "succeeded with a
                // failure outcome" so the caller propagates it.
                reportCoarseSmartShrinkOutcome(
                    plan: smartPlan,
                    iteration: iteration,
                    outcome: .abandoned,
                    windowIndex: windowIndex,
                    totalWindows: totalWindows
                )
                return .succeeded(.failure(retryStatus))
            }
        }

        // Iteration limit reached without success.
        reportCoarseSmartShrinkOutcome(
            plan: currentPlan,
            iteration: Self.coarseSmartShrinkMaxIterations,
            outcome: .abandoned,
            windowIndex: windowIndex,
            totalWindows: totalWindows
        )
        return .abandoned
    }

    private func reportCoarseSmartShrinkOutcome(
        plan: CoarsePassWindowPlan,
        iteration: Int,
        outcome: CoarsePassWindowDiagnostic.SmartShrinkOutcome,
        windowIndex: Int,
        totalWindows: Int
    ) {
        let preview = Self.coarsePromptPreview(plan.prompt)
        let diagnostic = CoarsePassWindowDiagnostic(
            kind: .smartShrinkOutcome,
            windowIndex: windowIndex,
            totalWindows: totalWindows,
            firstSegmentIndex: plan.lineRefs.first,
            lastSegmentIndex: plan.lineRefs.last,
            segmentCount: plan.lineRefs.count,
            promptTokens: plan.promptTokenCount,
            promptCharLength: plan.prompt.count,
            promptPreview: preview,
            errorDescription: "",
            errorReflect: "",
            status: outcome == .success ? .success : .exceededContextWindow,
            smartShrinkIteration: iteration,
            smartShrinkOutcome: outcome
        )
        logger.debug(
            """
            fm.classifier.coarse_pass_smart_shrink_outcome \
            window=\(windowIndex, privacy: .public) \
            totalWindows=\(totalWindows, privacy: .public) \
            iteration=\(iteration, privacy: .public) \
            outcome=\(outcome.rawValue, privacy: .public) \
            segmentCount=\(plan.lineRefs.count, privacy: .public) \
            promptTokens=\(plan.promptTokenCount, privacy: .public)
            """
        )
        Self.coarsePassDiagnosticObserver?(diagnostic)
    }

    private func refinementResponse(
        for plan: RefinementWindowPlan,
        sessionBox: SessionBox,
        lineRefLookup: [Int: AdTranscriptSegment]
    ) async -> RefinementResponseOutcome {
        do {
            return .success(plan: plan, schema: try await sessionBox.respondRefinement(plan.prompt))
        } catch {
            let status = SemanticScanStatus.from(error: error)
            reportRefinementDecodeFailureIfNeeded(
                plan: plan,
                error: error,
                status: status,
                retryStage: .initial
            )
            switch status.retryPolicy {
            case .shrinkWindowAndRetryOnce:
                guard let retryPlan = await shrunkenRefinementPlanForRetry(
                    from: plan,
                    lineRefLookup: lineRefLookup
                ) else {
                    return .failure(status)
                }
                do {
                    let response = try await sessionBox.respondRefinement(retryPlan.prompt)
                    return .success(plan: retryPlan, schema: response)
                } catch {
                    let retryStatus = SemanticScanStatus.from(error: error)
                    reportRefinementDecodeFailureIfNeeded(
                        plan: retryPlan,
                        error: error,
                        status: retryStatus,
                        retryStage: .shrinkRetry
                    )
                    return .failure(retryStatus)
                }
            case .backoffAndRetry:
                try? await Task.sleep(nanoseconds: 50_000_000)
                do {
                    let response = try await sessionBox.respondRefinement(plan.prompt)
                    return .success(plan: plan, schema: response)
                } catch {
                    let retryStatus = SemanticScanStatus.from(error: error)
                    reportRefinementDecodeFailureIfNeeded(
                        plan: plan,
                        error: error,
                        status: retryStatus,
                        retryStage: .backoffRetry
                    )
                    return .failure(retryStatus)
                }
            default:
                return .failure(status)
            }
        }
    }

    /// bd-3h2 diagnostic: when a refinement window resolves to
    /// `SemanticScanStatus.decodingFailure`, emit a structured breadcrumb
    /// (logger + test observer hook) so the root cause can be investigated
    /// without having to rerun the on-device benchmark blind. This is
    /// operational telemetry for a shadow-mode feature, so it logs at
    /// `.debug` level — loud enough to turn on during investigation, quiet
    /// enough not to pollute the default stream. All catchable refinement
    /// paths (initial, shrink retry, backoff retry) route through here so
    /// every decode failure leaves enough context in the log to triage
    /// without re-running.
    private func reportRefinementDecodeFailureIfNeeded(
        plan: RefinementWindowPlan,
        error: Error,
        status: SemanticScanStatus,
        retryStage: RefinementDecodeFailureDiagnostic.RetryStage
    ) {
        guard status == .decodingFailure else { return }

        let diagnostic = RefinementDecodeFailureDiagnostic(
            windowIndex: plan.windowIndex,
            sourceWindowIndex: plan.sourceWindowIndex,
            firstLineRef: plan.lineRefs.first,
            lastLineRef: plan.lineRefs.last,
            lineRefCount: plan.lineRefs.count,
            focusClusterCount: plan.focusClusters.count,
            promptTokenCount: plan.promptTokenCount,
            schemaName: Self.refinementSchemaName,
            stopReason: plan.stopReason,
            status: status,
            errorDescription: error.localizedDescription,
            errorDebugDescription: String(reflecting: error),
            retryStage: retryStage
        )

        logger.debug(
            """
            fm.classifier.refinement_decode_failure \
            stage=\(diagnostic.retryStage.rawValue, privacy: .public) \
            window=\(diagnostic.windowIndex, privacy: .public) \
            sourceWindow=\(diagnostic.sourceWindowIndex, privacy: .public) \
            lineRefs=[\(diagnostic.firstLineRef ?? -1, privacy: .public)..\(diagnostic.lastLineRef ?? -1, privacy: .public)] \
            lineRefCount=\(diagnostic.lineRefCount, privacy: .public) \
            clusters=\(diagnostic.focusClusterCount, privacy: .public) \
            promptTokens=\(diagnostic.promptTokenCount, privacy: .public) \
            schema=\(diagnostic.schemaName, privacy: .public) \
            stopReason=\(String(describing: diagnostic.stopReason), privacy: .public) \
            status=\(diagnostic.status.rawValue, privacy: .public) \
            error=\(diagnostic.errorDescription, privacy: .public) \
            debug=\(diagnostic.errorDebugDescription, privacy: .public)
            """
        )

        Self.refinementDecodeFailureObserver?(diagnostic)
    }

    /// bd-34e diagnostic: emit a structured breadcrumb on every coarse-pass
    /// window submission so investigators can correlate the prompt metadata
    /// (window index, segment span, char/token length, prompt preview) with
    /// any guardrail block that follows. The breadcrumb logs at `.debug` so
    /// it stays out of default Console.app filters until investigation turns
    /// it on. The companion observer hook lets unit tests capture events
    /// without scraping `os.Logger`.
    private func reportCoarsePassWindowSubmitIfNeeded(
        plan: CoarsePassWindowPlan,
        windowIndex: Int,
        totalWindows: Int
    ) {
        let preview = Self.coarsePromptPreview(plan.prompt)
        let diagnostic = CoarsePassWindowDiagnostic(
            kind: .submit,
            windowIndex: windowIndex,
            totalWindows: totalWindows,
            firstSegmentIndex: plan.lineRefs.first,
            lastSegmentIndex: plan.lineRefs.last,
            segmentCount: plan.lineRefs.count,
            promptTokens: plan.promptTokenCount,
            promptCharLength: plan.prompt.count,
            promptPreview: preview,
            errorDescription: "",
            errorReflect: "",
            status: nil
        )

        logger.debug(
            """
            fm.classifier.coarse_pass_window_submit \
            window=\(diagnostic.windowIndex, privacy: .public) \
            totalWindows=\(diagnostic.totalWindows, privacy: .public) \
            firstSegmentIndex=\(diagnostic.firstSegmentIndex ?? -1, privacy: .public) \
            lastSegmentIndex=\(diagnostic.lastSegmentIndex ?? -1, privacy: .public) \
            segmentCount=\(diagnostic.segmentCount, privacy: .public) \
            promptTokens=\(diagnostic.promptTokens, privacy: .public) \
            promptCharLength=\(diagnostic.promptCharLength, privacy: .public) \
            promptPreview=\(diagnostic.promptPreview, privacy: .public)
            """
        )

        Self.coarsePassDiagnosticObserver?(diagnostic)
    }

    /// bd-34e diagnostic: emit a structured breadcrumb in the catch arm of a
    /// coarse-pass window submission. Guardrail violations specifically are
    /// raised to `.error` so they show up in default Console.app filters
    /// during the on-device shadow benchmark; other error types stay at
    /// `.debug` to avoid log spam. `String(reflecting:)` surfaces Apple's
    /// `Context.debugDescription`, which often includes the actual safety
    /// reason that the localized description hides.
    private func reportCoarsePassErrorIfNeeded(
        plan: CoarsePassWindowPlan,
        error: Error,
        status: SemanticScanStatus,
        windowIndex: Int,
        totalWindows: Int
    ) {
        let preview = Self.coarsePromptPreview(plan.prompt)
        let diagnostic = CoarsePassWindowDiagnostic(
            kind: .error,
            windowIndex: windowIndex,
            totalWindows: totalWindows,
            firstSegmentIndex: plan.lineRefs.first,
            lastSegmentIndex: plan.lineRefs.last,
            segmentCount: plan.lineRefs.count,
            promptTokens: plan.promptTokenCount,
            promptCharLength: plan.prompt.count,
            promptPreview: preview,
            errorDescription: error.localizedDescription,
            errorReflect: String(reflecting: error),
            status: status
        )

        if status == .guardrailViolation {
            logger.error(
                """
                fm.classifier.coarse_pass_error \
                window=\(diagnostic.windowIndex, privacy: .public) \
                totalWindows=\(diagnostic.totalWindows, privacy: .public) \
                firstSegmentIndex=\(diagnostic.firstSegmentIndex ?? -1, privacy: .public) \
                lastSegmentIndex=\(diagnostic.lastSegmentIndex ?? -1, privacy: .public) \
                segmentCount=\(diagnostic.segmentCount, privacy: .public) \
                promptTokens=\(diagnostic.promptTokens, privacy: .public) \
                promptCharLength=\(diagnostic.promptCharLength, privacy: .public) \
                status=\(diagnostic.status?.rawValue ?? "unknown", privacy: .public) \
                errorDescription=\(diagnostic.errorDescription, privacy: .public) \
                errorReflect=\(diagnostic.errorReflect, privacy: .public) \
                promptPreview=\(diagnostic.promptPreview, privacy: .public)
                """
            )
        } else {
            logger.debug(
                """
                fm.classifier.coarse_pass_error \
                window=\(diagnostic.windowIndex, privacy: .public) \
                totalWindows=\(diagnostic.totalWindows, privacy: .public) \
                firstSegmentIndex=\(diagnostic.firstSegmentIndex ?? -1, privacy: .public) \
                lastSegmentIndex=\(diagnostic.lastSegmentIndex ?? -1, privacy: .public) \
                segmentCount=\(diagnostic.segmentCount, privacy: .public) \
                promptTokens=\(diagnostic.promptTokens, privacy: .public) \
                promptCharLength=\(diagnostic.promptCharLength, privacy: .public) \
                status=\(diagnostic.status?.rawValue ?? "unknown", privacy: .public) \
                errorDescription=\(diagnostic.errorDescription, privacy: .public) \
                errorReflect=\(diagnostic.errorReflect, privacy: .public) \
                promptPreview=\(diagnostic.promptPreview, privacy: .public)
                """
            )
        }

        Self.coarsePassDiagnosticObserver?(diagnostic)
    }

    /// bd-3h7: emit a supplementary breadcrumb when the error is an
    /// Apple `LanguageModelSession.GenerationError.refusal`. Captures a
    /// `String(reflecting:)` dump of the public `Refusal` value so the
    /// internal `TranscriptRecord` category that tripped the on-device
    /// safety classifier is visible in Console.app without a new build.
    /// No-ops for any other error type.
    private func reportCoarsePassRefusalDetailIfNeeded(
        plan: CoarsePassWindowPlan,
        error: Error,
        windowIndex: Int,
        totalWindows: Int
    ) {
        #if canImport(FoundationModels)
        guard #available(iOS 26.0, *) else { return }
        guard let generationError = error as? LanguageModelSession.GenerationError,
              case let .refusal(refusal, context) = generationError else {
            return
        }

        let refusalReflect = String(reflecting: refusal)
        let contextDebugDescription = context.debugDescription
        let preview = Self.coarsePromptPreview(plan.prompt)
        let diagnostic = CoarsePassWindowDiagnostic(
            kind: .refusalDetail,
            windowIndex: windowIndex,
            totalWindows: totalWindows,
            firstSegmentIndex: plan.lineRefs.first,
            lastSegmentIndex: plan.lineRefs.last,
            segmentCount: plan.lineRefs.count,
            promptTokens: plan.promptTokenCount,
            promptCharLength: plan.prompt.count,
            promptPreview: preview,
            errorDescription: contextDebugDescription,
            errorReflect: refusalReflect,
            status: .refusal
        )

        logger.notice(
            """
            fm.classifier.coarse_pass_refusal_detail \
            window=\(windowIndex, privacy: .public) \
            totalWindows=\(totalWindows, privacy: .public) \
            firstSegmentIndex=\(plan.lineRefs.first ?? -1, privacy: .public) \
            lastSegmentIndex=\(plan.lineRefs.last ?? -1, privacy: .public) \
            segmentCount=\(plan.lineRefs.count, privacy: .public) \
            contextDebugDescription=\(contextDebugDescription, privacy: .public) \
            recordReflect=\(refusalReflect, privacy: .public) \
            promptPreview=\(preview, privacy: .public)
            """
        )

        Self.coarsePassDiagnosticObserver?(diagnostic)
        #endif
    }

    /// bd-34e: render the first 200 characters of a coarse-pass prompt with
    /// newlines collapsed to spaces, so the structured log line stays
    /// single-line and grep-friendly.
    private static func coarsePromptPreview(_ prompt: String) -> String {
        let limit = 200
        let prefix = prompt.prefix(limit)
        var preview = String(prefix)
        preview = preview
            .replacingOccurrences(of: "\r\n", with: " ")
            .replacingOccurrences(of: "\n", with: " ")
            .replacingOccurrences(of: "\r", with: " ")
        return preview
    }

    private func shrunkenRefinementPlanForRetry(
        from plan: RefinementWindowPlan,
        lineRefLookup: [Int: AdTranscriptSegment]
    ) async -> RefinementWindowPlan? {
        let retryLineRefs = Array(Set(plan.focusLineRefs.filter { lineRefLookup[$0] != nil })).sorted()
        guard !retryLineRefs.isEmpty, retryLineRefs != plan.lineRefs else { return nil }

        let retrySegments = retryLineRefs.compactMap { lineRefLookup[$0] }
        guard retrySegments.count == retryLineRefs.count else { return nil }

        let retryEvidence = plan.promptEvidence.filter { retryLineRefs.contains($0.lineRef) }
        let retryPrompt = Self.buildRefinementPrompt(
            for: retrySegments,
            promptEvidence: retryEvidence,
            maximumSpans: config.maximumRefinementSpansPerWindow
        )
        let retryTokenCount = (try? await runtime.tokenCount(retryPrompt)) ?? plan.promptTokenCount

        return RefinementWindowPlan(
            windowIndex: plan.windowIndex,
            sourceWindowIndex: plan.sourceWindowIndex,
            lineRefs: retryLineRefs,
            focusLineRefs: retryLineRefs,
            focusClusters: buildFocusClusters(from: retryLineRefs),
            prompt: retryPrompt,
            promptTokenCount: retryTokenCount,
            startTime: retrySegments.first?.startTime ?? plan.startTime,
            endTime: retrySegments.last?.endTime ?? plan.endTime,
            stopReason: .tokenBudget,
            promptEvidence: retryEvidence
        )
    }

    /// bd-34e Fix B v3: build a single smart-shrunken coarse plan using
    /// Apple's reported actual token count. Returns `nil` only when we
    /// can't extract a count or when the input plan is already empty —
    /// the iterative caller (`runCoarseSmartShrinkLoop`) handles
    /// "shrunken plan still failed" by re-invoking with the new error.
    ///
    /// Targets `min(absoluteCeiling, contextSize / 2)` instead of the
    /// full content budget, leaving headroom for per-segment cost
    /// variance across attempts. Always shrinks the segment count by at
    /// least one when possible so progress is monotonic across
    /// iterations.
    private func smartShrunkenCoarsePlan(
        from plan: CoarsePassWindowPlan,
        lineRefLookup: [Int: AdTranscriptSegment],
        error: Error,
        windowIndex: Int,
        totalWindows: Int,
        iteration: Int
    ) async -> CoarsePassWindowPlan? {
        guard let actualTokens = Self.extractActualTokenCount(from: error) else {
            return nil
        }

        let segments = plan.lineRefs.compactMap { lineRefLookup[$0] }
        guard segments.count == plan.lineRefs.count, !segments.isEmpty else {
            return nil
        }

        let originalSegmentCount = segments.count
        let actualPerSegment = max(1, actualTokens / originalSegmentCount)

        // Compute Apple's real budget using the same overheads the
        // estimator already knows about. We deliberately do NOT apply
        // the bd-34e divisor here — `actualTokens` is already in Apple's
        // ground-truth units. bd-34e Fix B v3 caps the target at half
        // the context window so the retry leaves headroom for
        // per-segment cost variance from window to window.
        let contextSize = await runtime.contextSize()
        let schemaTokens = (try? await runtime.coarseSchemaTokenCount()) ?? 0
        let responseTokens = config.coarseMaximumResponseTokens
        let safetyMargin = config.safetyMarginTokens
        let absoluteCeiling = max(1, contextSize - schemaTokens - responseTokens - safetyMargin)
        let halvedContext = max(1, contextSize / 2)
        let targetTokens = min(absoluteCeiling, halvedContext)
        var targetSegments = max(1, targetTokens / actualPerSegment)

        // Force monotonic progress: if the math says we could keep the
        // same number of segments, drop one anyway so iteration N+1 is
        // strictly smaller than iteration N (until we hit 1 segment).
        if targetSegments >= originalSegmentCount && originalSegmentCount > 1 {
            targetSegments = originalSegmentCount - 1
        }

        let trimmedSegments = Array(segments.prefix(targetSegments))
        guard !trimmedSegments.isEmpty else { return nil }

        let trimmedPrompt = Self.buildPrompt(for: trimmedSegments)
        let trimmedTokenCount = (try? await runtime.tokenCount(trimmedPrompt)) ?? plan.promptTokenCount

        logger.debug(
            """
            fm.classifier.coarse_pass_smart_shrink \
            window=\(windowIndex, privacy: .public) \
            totalWindows=\(totalWindows, privacy: .public) \
            iteration=\(iteration, privacy: .public) \
            originalSegments=\(originalSegmentCount, privacy: .public) \
            actualTokens=\(actualTokens, privacy: .public) \
            actualPerSegment=\(actualPerSegment, privacy: .public) \
            targetTokens=\(targetTokens, privacy: .public) \
            targetSegments=\(targetSegments, privacy: .public)
            """
        )

        let attemptDiagnostic = CoarsePassWindowDiagnostic(
            kind: .smartShrinkAttempt,
            windowIndex: windowIndex,
            totalWindows: totalWindows,
            firstSegmentIndex: trimmedSegments.first?.segmentIndex,
            lastSegmentIndex: trimmedSegments.last?.segmentIndex,
            segmentCount: trimmedSegments.count,
            promptTokens: trimmedTokenCount,
            promptCharLength: trimmedPrompt.count,
            promptPreview: Self.coarsePromptPreview(trimmedPrompt),
            errorDescription: error.localizedDescription,
            errorReflect: String(reflecting: error),
            status: .exceededContextWindow,
            smartShrinkIteration: iteration,
            smartShrinkOutcome: nil
        )
        Self.coarsePassDiagnosticObserver?(attemptDiagnostic)

        return CoarsePassWindowPlan(
            windowIndex: 0,
            lineRefs: trimmedSegments.map(\.segmentIndex),
            prompt: trimmedPrompt,
            promptTokenCount: trimmedTokenCount,
            startTime: trimmedSegments.first?.startTime ?? plan.startTime,
            endTime: trimmedSegments.last?.endTime ?? plan.endTime,
            transcriptQuality: aggregateTranscriptQuality(for: trimmedSegments)
        )
    }

    private func shrunkenCoarsePlansForRetry(
        from plan: CoarsePassWindowPlan,
        lineRefLookup: [Int: AdTranscriptSegment]
    ) async -> [CoarsePassWindowPlan]? {
        let retrySegments = plan.lineRefs.compactMap { lineRefLookup[$0] }
        guard retrySegments.count == plan.lineRefs.count, retrySegments.count > 1 else {
            return nil
        }

        let midpoint = retrySegments.count / 2
        let chunks = [Array(retrySegments[..<midpoint]), Array(retrySegments[midpoint...])]
            .filter { !$0.isEmpty }
        guard chunks.count > 1 else { return nil }

        // M18: Each shrunken chunk must itself fit the prompt budget. If a
        // chunk shrinks to a single segment and STILL overflows, there is no
        // recovery — surface a hard failure by returning nil so the caller
        // converts to `.exceededContextWindow`.
        let budget = (try? await promptBudget()) ?? Int.max

        var retryPlans: [CoarsePassWindowPlan] = []
        retryPlans.reserveCapacity(chunks.count)

        for chunk in chunks {
            let prompt = Self.buildPrompt(for: chunk)
            let promptTokenCount = (try? await runtime.tokenCount(prompt)) ?? plan.promptTokenCount
            if promptTokenCount > budget {
                if chunk.count <= 1 {
                    // Hard failure — single segment still over budget.
                    logger.error(
                        "fm.classifier.coarse_retry_hard_failure tokens=\(promptTokenCount, privacy: .public) budget=\(budget, privacy: .public)"
                    )
                    return nil
                }
                // Recurse on the offending chunk to split it further.
                let nestedPlan = CoarsePassWindowPlan(
                    windowIndex: 0,
                    lineRefs: chunk.map(\.segmentIndex),
                    prompt: prompt,
                    promptTokenCount: promptTokenCount,
                    startTime: chunk.first?.startTime ?? plan.startTime,
                    endTime: chunk.last?.endTime ?? plan.endTime,
                    transcriptQuality: aggregateTranscriptQuality(for: chunk)
                )
                guard let nested = await shrunkenCoarsePlansForRetry(
                    from: nestedPlan,
                    lineRefLookup: lineRefLookup
                ) else {
                    return nil
                }
                retryPlans.append(contentsOf: nested)
                continue
            }
            retryPlans.append(
                CoarsePassWindowPlan(
                    windowIndex: 0,
                    lineRefs: chunk.map(\.segmentIndex),
                    prompt: prompt,
                    promptTokenCount: promptTokenCount,
                    startTime: chunk.first?.startTime ?? plan.startTime,
                    endTime: chunk.last?.endTime ?? plan.endTime,
                    transcriptQuality: aggregateTranscriptQuality(for: chunk)
                )
            )
        }

        return retryPlans
    }

    private func shouldRefine(_ disposition: CoarseDisposition) -> Bool {
        disposition == .containsAd || disposition == .uncertain
    }

    private func focusLineRefs(
        for window: FMCoarseWindowOutput,
        availableLineRefs: [Int]
    ) -> [Int] {
        let supportLineRefs = (window.screening.support?.supportLineRefs ?? [])
            .filter { availableLineRefs.contains($0) }
            .sorted()
        if supportLineRefs.isEmpty {
            return availableLineRefs
        }
        return supportLineRefs
    }

    private func buildFocusClusters(from lineRefs: [Int]) -> [[Int]] {
        guard let first = lineRefs.first else { return [] }

        var clusters: [[Int]] = [[first]]
        for lineRef in lineRefs.dropFirst() {
            let gap = lineRef - (clusters[clusters.count - 1].last ?? lineRef) - 1
            if gap <= config.zoomAmbiguityBudget {
                clusters[clusters.count - 1].append(lineRef)
            } else {
                clusters.append([lineRef])
            }
        }
        return clusters
    }

    private func expandedCluster(
        _ cluster: [Int],
        availableLineRefs: [Int]
    ) -> [Int] {
        var selected = cluster
        var selectedSet = Set(cluster)

        while selected.count < config.minimumZoomSpanLines {
            let first = selected.first ?? cluster.first ?? 0
            let last = selected.last ?? cluster.last ?? 0
            let rightCandidate = availableLineRefs.first(where: { $0 > last && !selectedSet.contains($0) })
            let leftCandidate = availableLineRefs.last(where: { $0 < first && !selectedSet.contains($0) })

            if let rightCandidate {
                selected.append(rightCandidate)
                selectedSet.insert(rightCandidate)
            } else if let leftCandidate {
                selected.insert(leftCandidate, at: 0)
                selectedSet.insert(leftCandidate)
            } else {
                break
            }
        }

        return selected
    }

    private func promptEvidenceEntries(
        for lineRefs: [Int],
        evidenceCatalog: EvidenceCatalog,
        lineRefByAtomOrdinal: [Int: Int]
    ) -> [PromptEvidenceEntry] {
        let allowedLineRefs = Set(lineRefs)
        return evidenceCatalog.entries
            .compactMap { entry in
                guard let lineRef = lineRefByAtomOrdinal[entry.atomOrdinal],
                      allowedLineRefs.contains(lineRef) else {
                    return nil
                }
                return PromptEvidenceEntry(entry: entry, lineRef: lineRef)
            }
            .sorted {
                if $0.lineRef == $1.lineRef {
                    return $0.entry.evidenceRef < $1.entry.evidenceRef
                }
                return $0.lineRef < $1.lineRef
            }
    }

    private func orderedSegmentsByLineRef(_ segments: [AdTranscriptSegment]) -> [AdTranscriptSegment] {
        segments.sorted { lhs, rhs in
            if lhs.segmentIndex == rhs.segmentIndex {
                return lhs.startTime < rhs.startTime
            }
            return lhs.segmentIndex < rhs.segmentIndex
        }
    }

    private func lineRefLookup(for segments: [AdTranscriptSegment]) -> [Int: AdTranscriptSegment] {
        // C3: Use uniquingKeysWith to avoid a hard crash if the input contains
        // two segments with the same segmentIndex. We keep the first one — the
        // ordering above is already deterministic by (segmentIndex, startTime).
        Dictionary(
            orderedSegmentsByLineRef(segments).map { ($0.segmentIndex, $0) },
            uniquingKeysWith: { first, _ in first }
        )
    }

    private func lineRefByAtomOrdinal(for segments: [AdTranscriptSegment]) -> [Int: Int] {
        var mapping: [Int: Int] = [:]
        for segment in orderedSegmentsByLineRef(segments) {
            for atom in segment.atoms {
                mapping[atom.atomKey.atomOrdinal] = segment.segmentIndex
            }
        }
        return mapping
    }

    private static func latencyMillis(
        since start: ContinuousClock.Instant,
        clock: ContinuousClock
    ) -> Double {
        let elapsed = clock.now - start
        return Double(elapsed.components.attoseconds) / 1e15 +
            Double(elapsed.components.seconds) * 1000.0
    }

    // H13: Strip Unicode control (Cc), format (Cf — includes BiDi marks like
    // U+202E and ZWJ U+200D), and U+2028/U+2029 line separators. Apply NFKC
    // normalization BEFORE escaping to prevent compatibility-character based
    // injection. Whitespace collapse is preserved.
    //
    // H14b: After Unicode scrubbing and whitespace collapse, defang any
    // smuggled transcript fence (`<<<TRANSCRIPT>>>` / `<<<END TRANSCRIPT>>>`)
    // and any forged `L<digits>>` line-ref prefix. A host that successfully
    // smuggled the verbatim close fence into a transcript line could close
    // the planner's fenced region prematurely; a forged `L42>` could
    // impersonate a real line ref. Both are rewritten to safe equivalents
    // that preserve the visible content but break the literal token boundary.
    static func escapedLine(_ text: String) -> String {
        // NFKC normalize first.
        let normalized = text.precomposedStringWithCompatibilityMapping

        // Filter dangerous categories.
        let scrubbed = String(normalized.unicodeScalars.compactMap { scalar -> Character? in
            // Drop U+2028 / U+2029 line separators.
            if scalar.value == 0x2028 || scalar.value == 0x2029 {
                return nil
            }
            // Drop control characters (Cc) including NUL.
            if scalar.properties.generalCategory == .control {
                return nil
            }
            // Drop format characters (Cf) — includes BiDi overrides, ZWJ, etc.
            if scalar.properties.generalCategory == .format {
                return nil
            }
            return Character(scalar)
        })

        let collapsed = scrubbed.split(whereSeparator: \.isWhitespace).joined(separator: " ")

        // H14b: Defang smuggled fences. Order matters — rewrite the close
        // fence first because it is a strict superstring of the open fence
        // would not be (open is `<<<TRANSCRIPT>>>`, close is
        // `<<<END TRANSCRIPT>>>`); doing close first keeps the rewrites
        // unambiguous regardless of order.
        let defangedFences = collapsed
            .replacingOccurrences(of: transcriptCloseFence, with: "«END TRANSCRIPT»")
            .replacingOccurrences(of: transcriptOpenFence, with: "«TRANSCRIPT»")

        // H14b: Defang forged `L<digits>>` line-ref prefixes by inserting a
        // space between the digits and `>`. The visible characters survive
        // but no downstream parser scanning for `L\d+>` will pick them up.
        let defangedLineRefs = Self.lineRefPrefixSmugglingPattern.stringByReplacingMatches(
            in: defangedFences,
            range: NSRange(defangedFences.startIndex..., in: defangedFences),
            withTemplate: "L$1 >"
        )

        return defangedLineRefs
            .replacingOccurrences(of: "\\", with: "\\\\")
            .replacingOccurrences(of: "\"", with: "\\\"")
    }

    // H14b: Compiled once at init to avoid recompiling per call. Matches
    // any `L<digits>>` token. Force-try is safe — the literal pattern is
    // valid and tested.
    private static let lineRefPrefixSmugglingPattern: NSRegularExpression = {
        // swiftlint:disable:next force_try
        try! NSRegularExpression(pattern: #"L(\d+)>"#)
    }()
}

// MARK: - Session Confinement
//
// H8: `LanguageModelSession` does not document `Sendable` conformance. We
// previously captured the session inside `@Sendable` closures, which crosses
// concurrency boundaries with no guarantee of thread safety. The session is
// now confined to a small actor and all `respond` / `prewarm` calls are
// dispatched through actor isolation.
final actor SessionBox {
    private let session: FoundationModelClassifier.Runtime.Session

    init(session: FoundationModelClassifier.Runtime.Session) {
        self.session = session
    }

    func prewarm(_ promptPrefix: String) async {
        await session.prewarm(promptPrefix)
    }

    func respondCoarse(_ prompt: String) async throws -> CoarseScreeningSchema {
        try await session.respondCoarse(prompt)
    }

    func respondRefinement(_ prompt: String) async throws -> RefinementWindowSchema {
        try await session.respondRefinement(prompt)
    }
}

#if canImport(FoundationModels)
@available(iOS 26.0, *)
final actor LiveSessionActor {
    private let session: LanguageModelSession

    init() {
        self.session = LanguageModelSession(model: SystemLanguageModel.default)
    }

    func prewarm(_ promptPrefix: String) {
        session.prewarm(promptPrefix: Prompt(promptPrefix))
    }

    func respondCoarse(_ prompt: String, maximumResponseTokens: Int) async throws -> CoarseScreeningSchema {
        let response = try await session.respond(
            to: prompt,
            generating: CoarseScreeningSchema.self,
            options: GenerationOptions(maximumResponseTokens: maximumResponseTokens)
        )
        return response.content
    }

    func respondRefinement(_ prompt: String, maximumResponseTokens: Int) async throws -> RefinementWindowSchema {
        let response = try await session.respond(
            to: prompt,
            generating: RefinementWindowSchema.self,
            options: GenerationOptions(maximumResponseTokens: maximumResponseTokens)
        )
        return response.content
    }
}
#endif

// MARK: - Live runtime

private extension FoundationModelClassifier {
    static func liveRuntime(logger: Logger, config: Config) -> Runtime {
        #if canImport(FoundationModels)
        Runtime(
            availabilityStatus: { locale in
                guard #available(iOS 26.0, *) else {
                    return .unavailable
                }

                let model = SystemLanguageModel.default
                if let status = SemanticScanStatus.from(availability: model.availability) {
                    return status
                }
                guard model.supportsLocale(locale) else {
                    return .unsupportedLocale
                }
                guard await FoundationModelsUsabilityProbe.probeIfNeeded(logger: logger) else {
                    return .failedTransient
                }
                return nil
            },
            contextSize: {
                guard #available(iOS 26.0, *) else { return 0 }
                return SystemLanguageModel.default.contextSize
            },
            tokenCount: { prompt in
                guard #available(iOS 26.0, *) else {
                    return fallbackTokenEstimate(for: prompt)
                }
                let model = SystemLanguageModel.default
                if #available(iOS 26.4, *) {
                    return try await model.tokenCount(for: prompt)
                }
                return fallbackTokenEstimate(for: prompt)
            },
            coarseSchemaTokenCount: {
                guard #available(iOS 26.0, *) else {
                    return fallbackCoarseSchemaTokenEstimate
                }
                let model = SystemLanguageModel.default
                if #available(iOS 26.4, *) {
                    return try await model.tokenCount(for: CoarseScreeningSchema.generationSchema)
                }
                return fallbackCoarseSchemaTokenEstimate
            },
            refinementSchemaTokenCount: {
                guard #available(iOS 26.0, *) else {
                    return fallbackRefinementSchemaTokenEstimate
                }
                let model = SystemLanguageModel.default
                if #available(iOS 26.4, *) {
                    return try await model.tokenCount(for: RefinementWindowSchema.generationSchema)
                }
                return fallbackRefinementSchemaTokenEstimate
            },
            makeSession: {
                guard #available(iOS 26.0, *) else {
                    return Runtime.Session(
                        prewarm: { _ in },
                        respondCoarse: { _ in
                            throw FoundationModelClassifierError.runtimeUnavailable("Foundation Models require iOS 26 or newer.")
                        },
                        respondRefinement: { _ in
                            throw FoundationModelClassifierError.runtimeUnavailable("Foundation Models require iOS 26 or newer.")
                        }
                    )
                }

                // H8: Confine the FoundationModels session to a small actor so
                // it never crosses concurrency boundaries via @Sendable closure
                // capture. The closures call into the actor instead.
                let live = LiveSessionActor()
                return Runtime.Session(
                    prewarm: { promptPrefix in
                        await live.prewarm(promptPrefix)
                    },
                    respondCoarse: { prompt in
                        try await live.respondCoarse(
                            prompt,
                            maximumResponseTokens: config.coarseMaximumResponseTokens
                        )
                    },
                    respondRefinement: { prompt in
                        try await live.respondRefinement(
                            prompt,
                            maximumResponseTokens: config.refinementMaximumResponseTokens
                        )
                    }
                )
            }
        )
        #else
        Runtime(
            availabilityStatus: { _ in .unavailable },
            contextSize: { 0 },
            tokenCount: { prompt in fallbackTokenEstimate(for: prompt) },
            coarseSchemaTokenCount: { fallbackCoarseSchemaTokenEstimate },
            refinementSchemaTokenCount: { fallbackRefinementSchemaTokenEstimate },
            makeSession: {
                Runtime.Session(
                    prewarm: { _ in },
                    respondCoarse: { _ in
                        throw FoundationModelClassifierError.runtimeUnavailable("FoundationModels framework not available.")
                    },
                    respondRefinement: { _ in
                        throw FoundationModelClassifierError.runtimeUnavailable("FoundationModels framework not available.")
                    }
                )
            }
        )
        #endif
    }

}
