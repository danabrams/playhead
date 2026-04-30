// EpisodeSummaryExtractor.swift
// playhead-jzik: on-device episode summary extractor.
//
// Pulls a bounded sample of transcript chunks (first / middle / last of
// the episode), feeds it to a `LanguageModelSession` against a
// schema-bound `@Generable` struct, and returns an `EpisodeSummary`
// for persistence by the backfill coordinator.
//
// Mirrors the FoundationModelClassifier patterns:
//   - `@available(iOS 26.0, *)`-gated production actor
//   - schema-bound generation when guardrails accept the prompt
//   - permissive-content fallback when the default safety classifier
//     refuses (matches the PermissiveAdClassifier path: free-form
//     `String` output through the relaxed guardrails, then a tiny
//     hand-rolled parser)
//   - testable via an injectable transport seam — production wires the
//     real `LanguageModelSession`; unit tests inject a mock that
//     responds with canned text
//
// The actor is intentionally NOT spliced into the AnalysisJobRunner /
// AnalysisWorkScheduler. Summaries are not part of the playback hot
// path (no skip cue depends on them) and the scheduler's job-row
// machinery would force schema migrations on `analysis_jobs` that this
// bead has no good reason to touch. The backfill coordinator
// (`EpisodeSummaryBackfillCoordinator`) drives this extractor on a
// quiet polling loop instead.

import Foundation
import OSLog

#if canImport(FoundationModels)
import FoundationModels
#endif

// MARK: - Sampling

/// Configuration for how `EpisodeSummaryExtractor` samples transcript
/// chunks before feeding them to the model. The defaults are chosen so
/// a typical 60-minute episode lands at well under 50K tokens of input
/// (the empirical ceiling we want to stay below to avoid context-window
/// retries on the FM).
struct EpisodeSummarySamplingConfig: Sendable, Equatable {
    /// Number of seconds from the start of the episode to include verbatim.
    let openingSeconds: Double
    /// Number of seconds at the end of the episode to include verbatim.
    let closingSeconds: Double
    /// Number of seconds of "middle" content to sample around the
    /// episode's midpoint. Centered on the midpoint of the transcript's
    /// covered range.
    let middleSeconds: Double
    /// Hard cap on the number of transcript-chunk text segments fed
    /// into the prompt. Even if the seconds-budget allows more, the
    /// extractor will subsample evenly so the prompt stays bounded.
    let maximumChunks: Int

    static let `default` = EpisodeSummarySamplingConfig(
        openingSeconds: 180,
        closingSeconds: 180,
        middleSeconds: 120,
        maximumChunks: 80
    )
}

/// Pure helper: select transcript chunks from `chunks` that fall inside
/// the opening/middle/closing windows defined by `config`. Returned
/// chunks are sorted by start time and de-duplicated by their `id`.
///
/// Lifted out of the actor so unit tests can exercise the sampling
/// surface without booting `LanguageModelSession`. The function tolerates
/// out-of-order or empty input and never throws — an empty input
/// produces an empty output.
enum EpisodeSummarySampler {
    static func sample(
        chunks: [TranscriptChunk],
        config: EpisodeSummarySamplingConfig = .default
    ) -> [TranscriptChunk] {
        guard !chunks.isEmpty else { return [] }
        let sorted = chunks.sorted { lhs, rhs in
            if lhs.startTime != rhs.startTime { return lhs.startTime < rhs.startTime }
            return lhs.chunkIndex < rhs.chunkIndex
        }
        let firstStart = sorted.first?.startTime ?? 0
        let lastEnd = sorted.last?.endTime ?? 0
        let totalDuration = max(0, lastEnd - firstStart)

        // Opening: chunks whose start time falls within the opening window.
        let openingCutoff = firstStart + config.openingSeconds
        let opening = sorted.filter { $0.startTime < openingCutoff }

        // Closing: chunks whose end time falls within the closing window.
        let closingCutoff = lastEnd - config.closingSeconds
        let closing = sorted.filter { $0.endTime > closingCutoff }

        // Middle: a window of `middleSeconds` centered on the midpoint
        // of the transcript's covered range. Skip when total duration
        // is small enough that opening + closing already overlap.
        let middle: [TranscriptChunk]
        if totalDuration > config.openingSeconds + config.closingSeconds + config.middleSeconds {
            let midpoint = firstStart + totalDuration / 2
            let middleStart = midpoint - config.middleSeconds / 2
            let middleEnd = midpoint + config.middleSeconds / 2
            middle = sorted.filter { chunk in
                chunk.endTime > middleStart && chunk.startTime < middleEnd
            }
        } else {
            middle = []
        }

        // Combine and de-duplicate by id, preserving sort order.
        var seen = Set<String>()
        var combined: [TranscriptChunk] = []
        combined.reserveCapacity(opening.count + middle.count + closing.count)
        for chunk in opening + middle + closing where !seen.contains(chunk.id) {
            seen.insert(chunk.id)
            combined.append(chunk)
        }
        combined.sort { $0.startTime < $1.startTime }

        // Hard cap on chunk count: even-stride subsample if oversized.
        guard combined.count > config.maximumChunks, config.maximumChunks > 0 else {
            return combined
        }
        var stride = Double(combined.count) / Double(config.maximumChunks)
        stride = max(stride, 1.0)
        var subsampled: [TranscriptChunk] = []
        var cursor: Double = 0
        while Int(cursor) < combined.count, subsampled.count < config.maximumChunks {
            subsampled.append(combined[Int(cursor)])
            cursor += stride
        }
        return subsampled
    }
}

// MARK: - Prompt grammar

/// Pure prompt builder + parser, lifted out of the actor so simulator
/// unit tests can exercise it without an iOS-26 availability gate. The
/// extractor's `@available` requirement comes from `LanguageModelSession`
/// itself; the grammar only touches Foundation.
enum EpisodeSummaryGrammar {

    /// Build the schema-bound prompt body. The model is instructed to
    /// stay strictly grounded in the transcript text and to leave fields
    /// empty when the source doesn't support a confident extraction.
    static func buildPrompt(
        episodeTitle: String?,
        showTitle: String?,
        chunks: [TranscriptChunk]
    ) -> String {
        let titleLine: String = {
            if let episodeTitle, !episodeTitle.isEmpty {
                let show = showTitle.map { " from \($0)" } ?? ""
                return "Episode title: \"\(episodeTitle)\"\(show)."
            } else if let showTitle, !showTitle.isEmpty {
                return "From the show: \"\(showTitle)\"."
            } else {
                return ""
            }
        }()

        let body = chunks
            .map { chunk -> String in
                let stamp = formatTimestamp(chunk.startTime)
                return "[\(stamp)] \(chunk.text.trimmingCharacters(in: .whitespacesAndNewlines))"
            }
            .joined(separator: "\n")

        let titleBlock = titleLine.isEmpty ? "" : titleLine + "\n\n"
        return """
        \(titleBlock)You are summarizing a podcast episode for a backlog-browsing screen.

        Below is a sampled transcript drawn from the opening, middle, and closing of the episode.

        Write a 2 to 3 sentence summary that captures what the episode is about. Keep the language concrete and grounded in what is actually said. Do not invent details, statistics, or quotes.

        Then list up to five short topic phrases that appear in the transcript. Each topic should be 1 to 4 words.

        Then list any notable guests or interviewees by name. Use only names that are clearly identified in the transcript. Leave the list empty if the episode is a solo monologue or no guest is named.

        Transcript:
        \(body)
        """
    }

    /// Build the permissive-content fallback prompt. Same content
    /// structure as the schema prompt, but asks for plain text in a
    /// hand-rolled grammar the parser below can read. Used when the
    /// schema-bound path refuses on guardrail content (medical, mental
    /// health, regulated topics).
    static func buildPermissivePrompt(
        episodeTitle: String?,
        showTitle: String?,
        chunks: [TranscriptChunk]
    ) -> String {
        let titleLine: String = {
            if let episodeTitle, !episodeTitle.isEmpty {
                let show = showTitle.map { " from \($0)" } ?? ""
                return "Episode title: \"\(episodeTitle)\"\(show)."
            } else if let showTitle, !showTitle.isEmpty {
                return "From the show: \"\(showTitle)\"."
            } else {
                return ""
            }
        }()

        let body = chunks
            .map { chunk -> String in
                let stamp = formatTimestamp(chunk.startTime)
                return "[\(stamp)] \(chunk.text.trimmingCharacters(in: .whitespacesAndNewlines))"
            }
            .joined(separator: "\n")

        let titleBlock = titleLine.isEmpty ? "" : titleLine + "\n\n"
        return """
        \(titleBlock)Summarize this podcast episode for a listener browsing their backlog.

        Output exactly three sections, in this order, with these literal headings on their own lines:
          SUMMARY:
          TOPICS:
          GUESTS:

        Under SUMMARY: write 2 to 3 sentences describing what the episode is about. Stay grounded in the transcript. Do not invent details.

        Under TOPICS: list up to five short topic phrases (1 to 4 words each), one per line, prefixed with a hyphen.

        Under GUESTS: list any guests or interviewees by name, one per line, prefixed with a hyphen. Leave the section blank if no guest is clearly named.

        Transcript:
        \(body)
        """
    }

    /// Parse a permissive-fallback response into its summary / topics /
    /// guests components. Lenient by design — empty sections, missing
    /// hyphen prefixes, and surrounding whitespace are all tolerated.
    static func parsePermissive(
        _ raw: String
    ) -> (summary: String, topics: [String], guests: [String]) {
        let trimmed = raw.trimmingCharacters(in: .whitespacesAndNewlines)
        guard !trimmed.isEmpty else { return ("", [], []) }

        var summaryLines: [String] = []
        var topicLines: [String] = []
        var guestLines: [String] = []

        enum Section { case none, summary, topics, guests }
        var current: Section = .none

        for rawLine in trimmed.split(whereSeparator: { $0.isNewline }) {
            let line = String(rawLine).trimmingCharacters(in: .whitespaces)
            guard !line.isEmpty else { continue }
            let upper = line.uppercased()
            if upper.hasPrefix("SUMMARY:") {
                current = .summary
                let after = String(line.dropFirst("SUMMARY:".count))
                    .trimmingCharacters(in: .whitespaces)
                if !after.isEmpty { summaryLines.append(after) }
                continue
            }
            if upper.hasPrefix("TOPICS:") {
                current = .topics
                continue
            }
            if upper.hasPrefix("GUESTS:") {
                current = .guests
                continue
            }
            switch current {
            case .summary:
                summaryLines.append(line)
            case .topics:
                topicLines.append(stripBullet(line))
            case .guests:
                guestLines.append(stripBullet(line))
            case .none:
                // Treat any leading prose before SUMMARY: as the summary
                // body — some responses skip the literal heading.
                summaryLines.append(line)
            }
        }

        let summary = summaryLines.joined(separator: " ")
            .trimmingCharacters(in: .whitespacesAndNewlines)
        let topics = topicLines.filter { !$0.isEmpty }
        let guests = guestLines.filter { !$0.isEmpty }
        return (summary, topics, guests)
    }

    private static func stripBullet(_ s: String) -> String {
        var trimmed = s.trimmingCharacters(in: .whitespaces)
        let prefixes = ["-", "•", "*", "·"]
        for prefix in prefixes where trimmed.hasPrefix(prefix) {
            trimmed = String(trimmed.dropFirst(prefix.count))
                .trimmingCharacters(in: .whitespaces)
        }
        // Leading numeric bullets like "1." or "1)".
        if let firstChar = trimmed.first, firstChar.isNumber {
            var i = trimmed.startIndex
            while i < trimmed.endIndex, trimmed[i].isNumber {
                i = trimmed.index(after: i)
            }
            if i < trimmed.endIndex, ".)".contains(trimmed[i]) {
                trimmed = String(trimmed[trimmed.index(after: i)...])
                    .trimmingCharacters(in: .whitespaces)
            }
        }
        return trimmed
    }

    private static func formatTimestamp(_ seconds: Double) -> String {
        let total = Int(max(0, seconds.rounded(.down)))
        let m = total / 60
        let s = total % 60
        return String(format: "%02d:%02d", m, s)
    }
}

// MARK: - Schema (FoundationModels-gated)

#if canImport(FoundationModels)
@available(iOS 26.0, *)
@Generable
struct EpisodeSummarySchema: Sendable, Codable, Hashable {
    var summary: String
    var mainTopics: [String]
    var notableGuests: [String]
}
#endif

// MARK: - Errors

/// Errors thrown by `EpisodeSummaryExtractor.extract`. The backfill
/// coordinator catches these, logs at debug, and decides whether to
/// retry on a future scan or shelve the asset.
enum EpisodeSummaryExtractionError: Error, Equatable, Sendable {
    /// Foundation Models is not currently usable — capability snapshot
    /// reports unavailable, the device lacks Apple Intelligence, locale
    /// is unsupported, etc. Backfill should retry once the capability
    /// changes.
    case capabilityUnavailable
    /// Transcript coverage was below the threshold the coordinator
    /// requires. The coordinator gates on this BEFORE calling the
    /// extractor; the error is here only for defense in depth.
    case insufficientCoverage
    /// Both the schema-bound path and the permissive fallback refused
    /// the prompt. The coordinator treats this as a terminal outcome
    /// for the current `transcriptVersion` and stops retrying.
    case bothPathsRefused
    /// The model returned content that we could not parse into a
    /// non-empty summary. Treated as transient — backfill may retry.
    case unparseableResponse
}

// MARK: - Transport seam

/// Narrow seam over the `LanguageModelSession` so unit tests can mock
/// the schema and permissive paths without booting the FM framework.
/// Production wires `LiveEpisodeSummaryTransport`, which mints a fresh
/// session per call (mirroring the per-call session lifecycle in
/// FoundationModelClassifier and PermissiveAdClassifier).
protocol EpisodeSummaryTransport: Sendable {

    /// Generate a structured summary against the schema-bound path.
    /// Throws `LanguageModelSession.GenerationError` on refusal /
    /// decoding-failure / context-overflow when iOS 26+ is available.
    /// In the simulator / no-FM build, throws `capabilityUnavailable`.
    func generateSchemaBound(prompt: String) async throws -> (
        summary: String,
        mainTopics: [String],
        notableGuests: [String]
    )

    /// Generate a free-form summary against the permissive guardrails
    /// path. Returns the raw response string for the caller to parse.
    func generatePermissive(prompt: String) async throws -> String
}

#if canImport(FoundationModels)
@available(iOS 26.0, *)
struct LiveEpisodeSummaryTransport: EpisodeSummaryTransport {
    private let logger: Logger

    init(logger: Logger = Logger(subsystem: "com.playhead", category: "EpisodeSummary")) {
        self.logger = logger
    }

    func generateSchemaBound(prompt: String) async throws -> (
        summary: String,
        mainTopics: [String],
        notableGuests: [String]
    ) {
        // Per-call session keeps the per-asset history bounded — the
        // same lesson the FoundationModelClassifier learned in bd-34e
        // (a long-lived session accumulates ~4000 tokens of conversation
        // history and starts hitting context-window overflows).
        let session = LanguageModelSession(model: SystemLanguageModel.default)
        let response = try await session.respond(
            to: prompt,
            generating: EpisodeSummarySchema.self,
            options: GenerationOptions(sampling: .greedy)
        )
        let schema = response.content
        return (schema.summary, schema.mainTopics, schema.notableGuests)
    }

    func generatePermissive(prompt: String) async throws -> String {
        let model = SystemLanguageModel(guardrails: .permissiveContentTransformations)
        let session = LanguageModelSession(model: model)
        let response = try await session.respond(
            to: prompt,
            options: GenerationOptions(sampling: .greedy)
        )
        return response.content.trimmingCharacters(in: .whitespacesAndNewlines)
    }
}
#endif

// MARK: - Capability provider seam

/// Narrow seam over the capability snapshot the extractor consults
/// before doing any FM work. Production wires
/// `CapabilitiesService.currentSnapshot.canUseFoundationModels`; tests
/// inject a stub.
protocol EpisodeSummaryCapabilityProvider: Sendable {
    func canUseFoundationModels() async -> Bool
}

/// Default production conformer. Wraps a `CapabilitiesProviding`
/// reference so the extractor does not need to know about the larger
/// capability surface.
struct CapabilitiesServiceEpisodeSummaryCapabilityProvider: EpisodeSummaryCapabilityProvider {
    let capabilities: any CapabilitiesProviding
    func canUseFoundationModels() async -> Bool {
        await capabilities.currentSnapshot.canUseFoundationModels
    }
}

// MARK: - EpisodeSummaryExtractor

/// Actor that produces a single `EpisodeSummary` from a sampled
/// transcript. Stateless across calls — every extraction mints its
/// own per-call session via the injected transport.
actor EpisodeSummaryExtractor {

    private let transport: any EpisodeSummaryTransport
    private let capability: any EpisodeSummaryCapabilityProvider
    private let samplingConfig: EpisodeSummarySamplingConfig
    private let logger: Logger

    /// Production initializer for hosts that already have a
    /// `CapabilitiesProviding`. The transport is provided externally so
    /// the iOS-26 availability gate is the caller's concern.
    init(
        transport: any EpisodeSummaryTransport,
        capability: any EpisodeSummaryCapabilityProvider,
        samplingConfig: EpisodeSummarySamplingConfig = .default,
        logger: Logger = Logger(subsystem: "com.playhead", category: "EpisodeSummary")
    ) {
        self.transport = transport
        self.capability = capability
        self.samplingConfig = samplingConfig
        self.logger = logger
    }

    /// Sampler accessor for tests. The actor's sampler is just a thin
    /// pass-through to `EpisodeSummarySampler`, exposed so test-suites
    /// can verify the sampled chunk count without instantiating the
    /// full extractor.
    nonisolated var samplingConfiguration: EpisodeSummarySamplingConfig {
        samplingConfig
    }

    /// Extract a summary for `assetId` given the supplied transcript
    /// chunks. The caller is responsible for fetching chunks and for
    /// gating on the ≥80%-coverage threshold; this method's job is
    /// purely to take a prepared chunk list and produce the summary.
    func extract(
        analysisAssetId: String,
        episodeTitle: String?,
        showTitle: String?,
        transcriptVersion: String?,
        chunks: [TranscriptChunk],
        now: Date = Date()
    ) async throws -> EpisodeSummary {
        guard await capability.canUseFoundationModels() else {
            throw EpisodeSummaryExtractionError.capabilityUnavailable
        }
        let sampled = EpisodeSummarySampler.sample(
            chunks: chunks,
            config: samplingConfig
        )
        guard !sampled.isEmpty else {
            throw EpisodeSummaryExtractionError.insufficientCoverage
        }

        // Schema-bound path first.
        do {
            let prompt = EpisodeSummaryGrammar.buildPrompt(
                episodeTitle: episodeTitle,
                showTitle: showTitle,
                chunks: sampled
            )
            let result = try await transport.generateSchemaBound(prompt: prompt)
            let summaryText = result.summary.trimmingCharacters(in: .whitespacesAndNewlines)
            guard !summaryText.isEmpty else {
                throw EpisodeSummaryExtractionError.unparseableResponse
            }
            let (topics, guests) = EpisodeSummary.sanitize(
                topics: result.mainTopics,
                guests: result.notableGuests
            )
            return EpisodeSummary(
                analysisAssetId: analysisAssetId,
                summary: summaryText,
                mainTopics: topics,
                notableGuests: guests,
                transcriptVersion: transcriptVersion,
                createdAt: now
            )
        } catch {
            // Decide whether to fall back to permissive. Refusal-shaped
            // errors warrant the fallback; everything else (cancellation,
            // unparseable, capability) propagates so the coordinator can
            // make a routing decision.
            if Task.isCancelled { throw CancellationError() }
            if !shouldFallBackToPermissive(after: error) {
                throw error
            }
            logger.debug("episode_summary_schema_path_refused, attempting permissive fallback")
        }

        // Permissive fallback — plain `String` output, hand-rolled parser.
        let permissivePrompt = EpisodeSummaryGrammar.buildPermissivePrompt(
            episodeTitle: episodeTitle,
            showTitle: showTitle,
            chunks: sampled
        )
        let raw: String
        do {
            raw = try await transport.generatePermissive(prompt: permissivePrompt)
        } catch is CancellationError {
            throw CancellationError()
        } catch {
            // Permissive ALSO refused / failed. Surface as
            // bothPathsRefused so the coordinator stops retrying for
            // this transcript version.
            logger.debug("episode_summary_permissive_path_failed: \(String(describing: error), privacy: .private)")
            throw EpisodeSummaryExtractionError.bothPathsRefused
        }
        let parsed = EpisodeSummaryGrammar.parsePermissive(raw)
        guard !parsed.summary.isEmpty else {
            throw EpisodeSummaryExtractionError.unparseableResponse
        }
        let (topics, guests) = EpisodeSummary.sanitize(
            topics: parsed.topics,
            guests: parsed.guests
        )
        return EpisodeSummary(
            analysisAssetId: analysisAssetId,
            summary: parsed.summary,
            mainTopics: topics,
            notableGuests: guests,
            transcriptVersion: transcriptVersion,
            createdAt: now
        )
    }

    /// Decide whether a thrown error from the schema-bound path should
    /// trigger a permissive fallback. Refusals get the fallback; other
    /// failure shapes (cancellation, capability vanishing mid-call,
    /// our own `unparseableResponse`) propagate untouched.
    private func shouldFallBackToPermissive(after error: Error) -> Bool {
        if error is CancellationError { return false }
        if let typed = error as? EpisodeSummaryExtractionError {
            // Our own `unparseableResponse` is a mid-call signal that
            // the schema-bound path returned empty content. Try the
            // permissive path — sometimes the model legitimately punted
            // on the schema and would have produced a usable plain-text
            // response with relaxed guardrails.
            return typed == .unparseableResponse
        }
        #if canImport(FoundationModels)
        if #available(iOS 26.0, *) {
            if let generation = error as? LanguageModelSession.GenerationError {
                switch generation {
                case .refusal:
                    return true
                case .decodingFailure:
                    // Decoding failures on the schema path frequently
                    // mean the safety classifier injected a refusal
                    // shape that didn't match the schema. Permissive
                    // is the right next step.
                    return true
                case .exceededContextWindowSize:
                    // Context-window failures mean prompt too big — the
                    // permissive path won't help (same prompt body).
                    return false
                @unknown default:
                    return false
                }
            }
        }
        #endif
        return false
    }
}
