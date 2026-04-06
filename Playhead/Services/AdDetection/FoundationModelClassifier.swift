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

enum FoundationModelClassifierError: Error, Sendable, LocalizedError {
    case segmentExceedsTokenBudget(lineRef: Int, tokenCount: Int, budget: Int)
    case runtimeUnavailable(String)

    var errorDescription: String? {
        switch self {
        case let .segmentExceedsTokenBudget(lineRef, tokenCount, budget):
            "Transcript segment \(lineRef) needs \(tokenCount) tokens, exceeding the coarse-pass budget of \(budget)."
        case let .runtimeUnavailable(reason):
            reason
        }
    }
}

// MARK: - Classifier

struct FoundationModelClassifier: Sendable {
    struct Config: Sendable {
        let safetyMarginTokens: Int
        let maximumResponseTokens: Int

        static let `default` = Config(
            safetyMarginTokens: 128,
            maximumResponseTokens: 96
        )
    }

    struct Runtime: Sendable {
        struct Session: Sendable {
            let prewarm: @Sendable (_ promptPrefix: String) async -> Void
            let respond: @Sendable (_ prompt: String) async throws -> CoarseScreeningSchema
        }

        let availabilityStatus: @Sendable (_ locale: Locale) async -> SemanticScanStatus?
        let contextSize: @Sendable () async -> Int
        let tokenCount: @Sendable (_ prompt: String) async throws -> Int
        let schemaTokenCount: @Sendable () async throws -> Int
        let makeSession: @Sendable () async -> Session
    }

    private static let promptPrefix = "Classify ad content."
    private static let fallbackSchemaTokenEstimate = 128

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
            let response = try await session.respond(plan.prompt)
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

    static func buildPrompt(for segments: [AdTranscriptSegment]) -> String {
        let transcriptLines = segments.map { segment in
            "\(segment.segmentIndex): \"\(escapedLine(segment.text))\""
        }

        return ([promptPrefix] + transcriptLines).joined(separator: "\n")
    }

    private func promptBudget() async throws -> Int {
        let contextSize = await runtime.contextSize()
        let schemaTokens = try await runtime.schemaTokenCount()
        return max(1, contextSize - schemaTokens - config.maximumResponseTokens - config.safetyMarginTokens)
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
            schemaTokenCount: {
                guard #available(iOS 26.0, *) else {
                    return fallbackSchemaTokenEstimate
                }
                let model = SystemLanguageModel.default
                if #available(iOS 26.4, *) {
                    return try await model.tokenCount(for: CoarseScreeningSchema.generationSchema)
                }
                return fallbackSchemaTokenEstimate
            },
            makeSession: {
                guard #available(iOS 26.0, *) else {
                    return Runtime.Session(
                        prewarm: { _ in },
                        respond: { _ in
                            throw FoundationModelClassifierError.runtimeUnavailable("Foundation Models require iOS 26 or newer.")
                        }
                    )
                }

                let session = LanguageModelSession(model: SystemLanguageModel.default)
                return Runtime.Session(
                    prewarm: { promptPrefix in
                        session.prewarm(promptPrefix: Prompt(promptPrefix))
                    },
                    respond: { prompt in
                        let response = try await session.respond(
                            to: prompt,
                            generating: CoarseScreeningSchema.self,
                            options: GenerationOptions(maximumResponseTokens: config.maximumResponseTokens)
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
            schemaTokenCount: { fallbackSchemaTokenEstimate },
            makeSession: {
                Runtime.Session(
                    prewarm: { _ in },
                    respond: { _ in
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
