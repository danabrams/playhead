import Foundation
import OSLog

#if canImport(FoundationModels)
import FoundationModels
#endif

// MARK: - Coarse screening schema

#if canImport(FoundationModels)
@available(iOS 26.0, *)
@Generable
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
    @Guide(description: "Line reference integers from the quoted transcript that directly support the disposition.")
    var supportLineRefs: [Int]

    @Guide(description: "Calibrated certainty band for the support.")
    var certainty: CertaintyBand
}

@available(iOS 26.0, *)
@Generable
struct CoarseScreeningSchema: Sendable, Codable, Hashable {
    @Guide(description: "Transcript quality for this window. Use unusable when the quoted transcript is too degraded to classify reliably.")
    var transcriptQuality: TranscriptQuality

    @Guide(description: "Coarse ad-screening disposition for the window.")
    var disposition: CoarseDisposition

    @Guide(description: "Optional supporting line references and certainty band. Leave null when abstaining or when no specific support lines apply.")
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

@available(iOS 26.0, *)
@Generable
enum AlternativeExplanation: String, Sendable, Codable, Hashable {
    case none
    case editorialContext
    case guestPromotion
    case showCredits
    case unknown
}

@available(iOS 26.0, *)
@Generable
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
    var alternativeExplanation: AlternativeExplanation
    var reasonTags: [ReasonTag]
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
    var transcriptQuality: TranscriptQuality
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
    var alternativeExplanation: AlternativeExplanation
    var reasonTags: [ReasonTag]
}

struct RefinementWindowSchema: Sendable, Codable, Hashable {
    var spans: [SpanRefinementSchema]
}
#endif

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
    let screening: CoarseScreeningSchema
    let latencyMillis: Double
}

struct FMCoarseScanOutput: Sendable, Equatable {
    let status: SemanticScanStatus
    let windows: [FMCoarseWindowOutput]
    let latencyMillis: Double
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
    let entry: EvidenceEntry
    let lineRef: Int
    let kind: EvidenceCategory
    let certainty: CertaintyBand
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
    private static let fallbackCoarseSchemaTokenEstimate = 128
    private static let fallbackRefinementSchemaTokenEstimate = 256

    private let runtime: Runtime
    private let config: Config

    init(
        runtime: Runtime? = nil,
        config: Config = .default,
        logger: Logger = Logger(subsystem: "com.playhead", category: "FoundationModelClassifier")
    ) {
        self.config = config
        self.runtime = runtime ?? Self.liveRuntime(logger: logger, config: config)
    }

    func coarsePassA(
        segments: [AdTranscriptSegment],
        locale: Locale = .current
    ) async throws -> FMCoarseScanOutput {
        let clock = ContinuousClock()
        let start = clock.now

        if let status = await runtime.availabilityStatus(locale) {
            return FMCoarseScanOutput(status: status, windows: [], latencyMillis: 0)
        }

        let plans = try await planPassA(segments: segments)
        guard !plans.isEmpty else {
            return FMCoarseScanOutput(
                status: .success,
                windows: [],
                latencyMillis: 0
            )
        }

        let prewarmSession = await runtime.makeSession()
        await prewarmSession.prewarm(Self.promptPrefix)

        var windows: [FMCoarseWindowOutput] = []
        windows.reserveCapacity(plans.count)

        for plan in plans {
            let session = await runtime.makeSession()
            let windowStart = clock.now
            let response = try await session.respondCoarse(plan.prompt)
            let screening = sanitize(
                schema: response,
                validLineRefs: Set(plan.lineRefs),
                transcriptQuality: plan.transcriptQuality
            )
            windows.append(
                FMCoarseWindowOutput(
                    windowIndex: plan.windowIndex,
                    lineRefs: plan.lineRefs,
                    startTime: plan.startTime,
                    endTime: plan.endTime,
                    screening: screening,
                    latencyMillis: Self.latencyMillis(since: windowStart, clock: clock)
                )
            )
        }

        return FMCoarseScanOutput(
            status: .success,
            windows: windows,
            latencyMillis: Self.latencyMillis(since: start, clock: clock)
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
            return FMRefinementScanOutput(status: status, windows: [], latencyMillis: 0)
        }

        guard !zoomPlans.isEmpty else {
            return FMRefinementScanOutput(status: .success, windows: [], latencyMillis: 0)
        }

        let lineRefLookup = lineRefLookup(for: segments)
        let prewarmSession = await runtime.makeSession()
        await prewarmSession.prewarm(Self.refinementPromptPrefix)

        var windows: [FMRefinementWindowOutput] = []
        windows.reserveCapacity(zoomPlans.count)

        for plan in zoomPlans {
            let session = await runtime.makeSession()
            let windowStart = clock.now
            let response = try await session.respondRefinement(plan.prompt)
            let spans = sanitize(
                schema: response,
                plan: plan,
                lineRefLookup: lineRefLookup,
                evidenceCatalog: evidenceCatalog
            )
            windows.append(
                FMRefinementWindowOutput(
                    windowIndex: plan.windowIndex,
                    sourceWindowIndex: plan.sourceWindowIndex,
                    lineRefs: plan.lineRefs,
                    spans: spans,
                    latencyMillis: Self.latencyMillis(since: windowStart, clock: clock)
                )
            )
        }

        return FMRefinementScanOutput(
            status: .success,
            windows: windows,
            latencyMillis: Self.latencyMillis(since: start, clock: clock)
        )
    }

    static func buildPrompt(for segments: [AdTranscriptSegment]) -> String {
        let transcriptLines = segments.map { segment in
            "\(segment.segmentIndex): \"\(escapedLine(segment.text))\""
        }

        return ([promptPrefix] + transcriptLines).joined(separator: "\n")
    }

    private static func buildRefinementPrompt(
        for segments: [AdTranscriptSegment],
        promptEvidence: [PromptEvidenceEntry],
        maximumSpans: Int
    ) -> String {
        var lines = [refinementPromptPrefix, "Transcript:"]
        lines.append(contentsOf: segments.map { segment in
            "\(segment.segmentIndex): \"\(escapedLine(segment.text))\""
        })
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
            maximumResponseTokens: config.coarseMaximumResponseTokens
        )
    }

    private func promptBudget(
        schemaTokens: @escaping @Sendable () async throws -> Int,
        maximumResponseTokens: Int
    ) async throws -> Int {
        let contextSize = await runtime.contextSize()
        let schemaTokenCount = try await schemaTokens()
        return max(1, contextSize - schemaTokenCount - maximumResponseTokens - config.safetyMarginTokens)
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

    private func sanitize(
        schema: CoarseScreeningSchema,
        validLineRefs: Set<Int>,
        transcriptQuality: TranscriptQuality
    ) -> CoarseScreeningSchema {
        let sanitizedSupport: CoarseSupportSchema?
        switch schema.disposition {
        case .noAds, .abstain:
            sanitizedSupport = nil
        case .containsAd, .uncertain:
            if let support = schema.support {
                var deduped: [Int] = []
                var seen: Set<Int> = []
                for lineRef in support.supportLineRefs where validLineRefs.contains(lineRef) {
                    if seen.insert(lineRef).inserted {
                        deduped.append(lineRef)
                    }
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
            transcriptQuality: transcriptQuality,
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
                    resolvedEvidenceAnchors: resolveEvidenceAnchors(
                        span.evidenceAnchors,
                        plan: plan,
                        lineRefLookup: lineRefLookup,
                        evidenceCatalog: evidenceCatalog
                    ),
                    alternativeExplanation: span.alternativeExplanation,
                    reasonTags: Array(Set(span.reasonTags)).sorted { $0.rawValue < $1.rawValue }
                )
            )
        }

        return spans
    }

    private func resolveEvidenceAnchors(
        _ anchors: [EvidenceAnchorSchema],
        plan: RefinementWindowPlan,
        lineRefLookup: [Int: AdTranscriptSegment],
        evidenceCatalog: EvidenceCatalog
    ) -> [ResolvedEvidenceAnchor] {
        anchors.compactMap { anchor in
            if let evidenceRef = anchor.evidenceRef,
               let promptEntry = plan.promptEvidence.first(where: { $0.entry.evidenceRef == evidenceRef }) {
                return ResolvedEvidenceAnchor(
                    entry: promptEntry.entry,
                    lineRef: promptEntry.lineRef,
                    kind: promptEntry.entry.category,
                    certainty: anchor.certainty
                )
            }

            guard let segment = lineRefLookup[anchor.lineRef] else {
                return nil
            }
            guard let entry = evidenceCatalog.entries.first(where: { entry in
                segment.atoms.contains(where: { $0.atomKey.atomOrdinal == entry.atomOrdinal }) &&
                entry.category == anchor.kind.category
            }) else {
                return nil
            }
            return ResolvedEvidenceAnchor(
                entry: entry,
                lineRef: anchor.lineRef,
                kind: entry.category,
                certainty: anchor.certainty
            )
        }
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
        Dictionary(uniqueKeysWithValues: orderedSegmentsByLineRef(segments).map { ($0.segmentIndex, $0) })
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

    private static func escapedLine(_ text: String) -> String {
        let collapsed = text.split(whereSeparator: \.isWhitespace).joined(separator: " ")
        return collapsed
            .replacingOccurrences(of: "\\", with: "\\\\")
            .replacingOccurrences(of: "\"", with: "\\\"")
    }
}

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

                let session = LanguageModelSession(model: SystemLanguageModel.default)
                return Runtime.Session(
                    prewarm: { promptPrefix in
                        session.prewarm(promptPrefix: Prompt(promptPrefix))
                    },
                    respondCoarse: { prompt in
                        let response = try await session.respond(
                            to: prompt,
                            generating: CoarseScreeningSchema.self,
                            options: GenerationOptions(maximumResponseTokens: config.coarseMaximumResponseTokens)
                        )
                        return response.content
                    },
                    respondRefinement: { prompt in
                        let response = try await session.respond(
                            to: prompt,
                            generating: RefinementWindowSchema.self,
                            options: GenerationOptions(maximumResponseTokens: config.refinementMaximumResponseTokens)
                        )
                        return response.content
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

    static func fallbackTokenEstimate(for prompt: String) -> Int {
        let wordCount = prompt.split(whereSeparator: \.isWhitespace).count
        return max(1, Int(ceil(Double(wordCount) * 1.35)))
    }
}

private extension EvidenceAnchorKind {
    var category: EvidenceCategory {
        switch self {
        case .url:
            .url
        case .promoCode:
            .promoCode
        case .ctaPhrase:
            .ctaPhrase
        case .disclosurePhrase:
            .disclosurePhrase
        case .brandSpan:
            .brandSpan
        }
    }
}
