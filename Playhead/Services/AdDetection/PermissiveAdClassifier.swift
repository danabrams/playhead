import Foundation
import OSLog

#if canImport(FoundationModels)
import FoundationModels
#endif

/// bd-1en Phase 1: classify a window of transcript segments through
/// Apple's `SystemLanguageModel(guardrails: .permissiveContentTransformations)`
/// path with a hand-rolled string-output grammar.
///
/// Why a separate classifier:
///
/// - Apple's `.permissiveContentTransformations` mode relaxes the safety
///   classifier for text-to-text transformation tasks but ONLY when the
///   model is generating a `String`. `@Generable` guided generation
///   always runs the default guardrails. So pharma / medical / mental-
///   health windows that historically refuse on the existing
///   `FoundationModelClassifier.coarsePassA` path need to skip the
///   `@Generable` schema entirely and use plain string output instead.
///
/// - The 124-probe validation matrix in
///   `PlayheadFMSmokeTests::testPermissiveTransformationProbeMatrix`
///   confirmed empirically that the permissive path recovers every
///   known-pharma probe (CVS pre-roll family, Trulicity, Ozempic,
///   Rinvoq, BetterHelp, regulated medical tests) with zero
///   `GenerationError.refusal` cases.
///
/// API contract:
///
/// - The classifier returns `CoarseScreeningSchema` — the SAME type the
///   existing `FoundationModelClassifier.coarsePassA` produces — so
///   downstream consumers (refinement planning, persistence, evidence
///   resolution) don't need to know which path produced the result.
///
/// - On any catastrophic failure (`refusal`, `decodingFailure`,
///   `exceededContextWindowSize`, or unknown `GenerationError`) the
///   classifier returns `.uncertain` instead of throwing. The downstream
///   pipeline already knows how to drop uncertain windows; bubbling
///   the error up would force the entire backfill job to abort.
///
/// - Per-call sessions: the actor holds a single `SystemLanguageModel`
///   reference (cheap — keeps model assets warm), but constructs a
///   fresh `LanguageModelSession` per call. This mirrors bd-34e Fix B
///   v5 on the standard coarse path: a shared session accumulates
///   ~4000 tokens of conversation history after ~7 successful exchanges
///   and starts hitting `GenerationError.exceededContextWindowSize`.
///   The validation test (commit 57601f3) saw exactly this bug when it
///   reused a single session across all 124 probes.
///
/// - Greedy sampling: `GenerationOptions(sampling: .greedy)` removes
///   ordinary decoding randomness so the same window deterministically
///   produces the same response across runs.
/// bd-1en Phase 1 parser/prompt namespace. Lifted out of the actor so
/// the simulator unit-test suite can call it without an iOS 26
/// availability gate (the FoundationModels framework is the only
/// reason `PermissiveAdClassifier` itself is gated). The parser and
/// prompt builder do not touch FoundationModels at all — they're
/// pure Foundation/regex helpers.
/// Result of running the permissive refinement pass on a single window.
///
/// - `.spans`    → one `RefinedAdSpan` per pair (anchorless by design;
///                  the permissive path omits the evidence catalog).
/// - `.noAd`     → drop the window. The focused refinement contradicted
///                  the coarse verdict; trust the more focused signal.
/// - `.unparsed` → fall back to a single full-window `RefinedAdSpan`.
///                  Recall safety net: parser failures must never cost
///                  us an ad we already coarse-confirmed.
enum PermissiveRefinementResult: Sendable, Equatable {
    case spans([RefinementSpanPair])
    case noAd
    case unparsed
}

extension PermissiveRefinementResult: CustomStringConvertible {
    var description: String {
        switch self {
        case .spans: return "spans"
        case .noAd: return "noAd"
        case .unparsed: return "unparsed"
        }
    }
}

extension PermissiveRefinementResult {
    /// Convert this result into the `[RefinedAdSpan]` shape the runner
    /// persists. Bypasses `FoundationModelClassifier.sanitize` because
    /// that path rejects anchorless spans outright.
    ///
    /// `.unparsed` collapses to a single full-window span: coarse
    /// already classified the window as containsAd via the permissive
    /// path, so losing the ad to a parser glitch is strictly worse
    /// than emitting a slightly-too-wide span.
    func refinedSpans(
        for plan: RefinementWindowPlan,
        lineRefLookup: [Int: AdTranscriptSegment]
    ) -> [RefinedAdSpan] {
        switch self {
        case .noAd:
            return []
        case let .spans(pairs):
            return pairs.compactMap { pair in
                guard let firstSegment = lineRefLookup[pair.firstLineRef],
                      let lastSegment = lineRefLookup[pair.lastLineRef] else {
                    return nil
                }
                return Self.makeAnchorlessSpan(
                    firstLineRef: pair.firstLineRef,
                    lastLineRef: pair.lastLineRef,
                    firstSegment: firstSegment,
                    lastSegment: lastSegment,
                    boundaryPrecision: .usable
                )
            }
        case .unparsed:
            guard let firstLineRef = plan.lineRefs.first,
                  let lastLineRef = plan.lineRefs.last,
                  let firstSegment = lineRefLookup[firstLineRef],
                  let lastSegment = lineRefLookup[lastLineRef] else {
                return []
            }
            return [
                Self.makeAnchorlessSpan(
                    firstLineRef: firstLineRef,
                    lastLineRef: lastLineRef,
                    firstSegment: firstSegment,
                    lastSegment: lastSegment,
                    boundaryPrecision: .rough
                )
            ]
        }
    }

    /// Defaults: `.paid` + `.thirdParty` because the router only fires
    /// on coarse-confirmed sponsor-shaped pharma content. `.strong`
    /// because coarse already said containsAd. `memoryWriteEligible`
    /// is always false — anchorless spans never write to sponsor memory.
    private static func makeAnchorlessSpan(
        firstLineRef: Int,
        lastLineRef: Int,
        firstSegment: AdTranscriptSegment,
        lastSegment: AdTranscriptSegment,
        boundaryPrecision: BoundaryPrecision
    ) -> RefinedAdSpan {
        RefinedAdSpan(
            commercialIntent: .paid,
            ownership: .thirdParty,
            firstLineRef: firstLineRef,
            lastLineRef: lastLineRef,
            firstAtomOrdinal: firstSegment.firstAtomOrdinal,
            lastAtomOrdinal: lastSegment.lastAtomOrdinal,
            certainty: .strong,
            boundaryPrecision: boundaryPrecision,
            resolvedEvidenceAnchors: [],
            memoryWriteEligible: false,
            alternativeExplanation: .unknown,
            reasonTags: []
        )
    }
}

/// A `(firstLineRef, lastLineRef)` pair extracted from a permissive
/// refinement response. Each pair becomes one `RefinedAdSpan`. Inclusive
/// on both ends.
struct RefinementSpanPair: Sendable, Equatable, Hashable {
    let firstLineRef: Int
    let lastLineRef: Int
}

enum PermissiveAdGrammar {

    /// Build the production permissive prompt for a window. The
    /// validation test's prompt parrots the literal grammar template
    /// (the model would happily echo back `AD L<start>-L<end>` instead
    /// of substituting concrete numbers); the production prompt forces
    /// concrete line refs by spelling out the actual line ref list and
    /// providing illustrative examples.
    static func buildPrompt(for segments: [AdTranscriptSegment]) -> String {
        let lineRefs = segments.map(\.segmentIndex)
        let lineRefList = lineRefs.map { "L\($0)" }.joined(separator: ", ")
        let transcriptBody = segments
            .map { "L\($0.segmentIndex)> \"\($0.text)\"" }
            .joined(separator: "\n")
        return """
        You are analyzing a podcast transcript window for advertising content.

        The transcript below has these line refs: \(lineRefList). Use ONLY these line refs in your answer.

        Output exactly one line. Choose ONE of these forms:

          NO_AD                              (window contains no ad)
          UNCERTAIN                          (you cannot tell)
          AD L<start>-L<end>                 (one ad span)
          AD L<n1>-L<m1>,L<n2>-L<m2>         (multiple non-contiguous ad spans)

        Do NOT output literal text like "L<n>" or "L<start>" — substitute actual line ref numbers from the transcript above.
        Do NOT echo or paraphrase the transcript.
        Do NOT explain your reasoning.
        Do NOT use line refs that are not in the transcript.

        Examples (these are illustrative, not part of your input):
          Transcript with L0, L1 → if both lines are an ad, output "AD L0-L1"
          Transcript with L4, L5 → if neither is an ad, output "NO_AD"
          Transcript with L10, L11, L12 → if L10-L11 is one ad and L12 is unrelated, output "AD L10-L11"

        Now classify this transcript:

        \(transcriptBody)
        """
    }

    /// Parse the model's response into a `CoarseScreeningSchema`.
    ///
    /// Grammar (case-insensitive, first non-empty line only):
    ///
    ///   NO_AD                                   → .noAds
    ///   UNCERTAIN                               → .uncertain
    ///   AD L<n>-L<m>[,L<p>-L<q>]...             → .containsAd
    ///   anything else                           → .uncertain
    ///
    /// Validation: parsed line refs are intersected with `validLineRefs`
    /// (the actual segment indices in the window). Refs that fall
    /// outside the window are dropped. If the intersection is empty
    /// after dropping, the disposition collapses to `.uncertain` —
    /// this is what catches the model returning template literal
    /// `"AD L<start>-L<end>"` (no real numbers parse out) and the case
    /// where the model hallucinates line refs that don't exist in the
    /// window.
    static func parse(_ raw: String, validLineRefs: [Int]) -> CoarseScreeningSchema {
        let trimmed = raw.trimmingCharacters(in: .whitespacesAndNewlines)
        guard !trimmed.isEmpty else {
            return CoarseScreeningSchema(disposition: .uncertain, support: nil)
        }

        // First non-empty line only.
        let firstNonEmptyLine: String? = trimmed
            .split(whereSeparator: { $0.isNewline })
            .first(where: { !$0.trimmingCharacters(in: .whitespaces).isEmpty })
            .map { String($0).trimmingCharacters(in: .whitespaces) }
        guard let line = firstNonEmptyLine, !line.isEmpty else {
            return CoarseScreeningSchema(disposition: .uncertain, support: nil)
        }

        let upper = line.uppercased()

        if upper == "NO_AD" {
            return CoarseScreeningSchema(disposition: .noAds, support: nil)
        }
        if upper == "UNCERTAIN" {
            return CoarseScreeningSchema(disposition: .uncertain, support: nil)
        }

        // AD form. Require the literal "AD " prefix followed by at
        // least one well-formed L<digits>-L<digits> pair. The pair
        // regex is anchored on actual digits; template literals like
        // `L<start>` will not match because `<start>` is not `\d+`.
        guard upper.hasPrefix("AD ") else {
            return CoarseScreeningSchema(disposition: .uncertain, support: nil)
        }

        // Extract the body after "AD ".
        let body = String(line.dropFirst(3))
        let pairs = parseAdPairs(body)
        guard !pairs.isEmpty else {
            // Model returned "AD <something not parseable>" — likely
            // template parroting. Collapse to uncertain.
            return CoarseScreeningSchema(disposition: .uncertain, support: nil)
        }

        // Expand each (start, end) pair into the integer range.
        var expanded: Set<Int> = []
        for (start, end) in pairs {
            let lo = min(start, end)
            let hi = max(start, end)
            for i in lo...hi {
                expanded.insert(i)
            }
        }

        // Validate against the actual window's line refs.
        let validSet = Set(validLineRefs)
        let intersection = expanded.intersection(validSet)
        guard !intersection.isEmpty else {
            return CoarseScreeningSchema(disposition: .uncertain, support: nil)
        }

        let supportLineRefs = intersection.sorted()
        return CoarseScreeningSchema(
            disposition: .containsAd,
            support: CoarseSupportSchema(
                supportLineRefs: supportLineRefs,
                certainty: .strong
            )
        )
    }

    /// Refinement variant of `buildPrompt`. Asks the model for the
    /// *tightest* contiguous line ref range(s). Deliberately omits the
    /// evidence catalog — the catalog snippet is what trips refusals
    /// on the standard refinement path.
    static func buildRefinementPrompt(for segments: [AdTranscriptSegment]) -> String {
        let lineRefs = segments.map(\.segmentIndex)
        let lineRefList = lineRefs.map { "L\($0)" }.joined(separator: ", ")
        let transcriptBody = segments
            .map { "L\($0.segmentIndex)> \"\($0.text)\"" }
            .joined(separator: "\n")
        return """
        You are refining the ad span boundaries inside a podcast transcript window.

        The transcript below has these line refs: \(lineRefList). Use ONLY these line refs in your answer.

        Output exactly one line. Choose ONE of these forms:

          NO_AD                              (window contains no ad)
          UNCERTAIN                          (you cannot tell)
          AD L<start>-L<end>                 (one ad span)
          AD L<n1>-L<m1>,L<n2>-L<m2>         (multiple non-contiguous ad spans)

        Identify the TIGHTEST contiguous line ref range(s) that are part of an ad / sponsorship / promotional read. Do not include surrounding host content.

        Do NOT output literal text like "L<n>" or "L<start>" — substitute actual line ref numbers from the transcript above.
        Do NOT echo or paraphrase the transcript.
        Do NOT explain your reasoning.
        Do NOT use line refs that are not in the transcript.

        Examples (these are illustrative, not part of your input):
          Transcript with L0, L1, L2 → if L0-L1 is the ad and L2 is host content, output "AD L0-L1"
          Transcript with L4, L5 → if neither is an ad, output "NO_AD"
          Transcript with L10, L11, L12 → if L10 and L12 are unrelated ads, output "AD L10-L10,L12-L12"

        Now refine this transcript:

        \(transcriptBody)
        """
    }

    /// Refinement variant of `parse`. Preserves pair structure (so
    /// non-contiguous spans become separate `RefinedAdSpan`s), clamps
    /// each pair to `validLineRefs`, and snaps endpoints inward when
    /// the window has planner gaps. Garbage / template-parroting /
    /// refusal-shaped output collapses to `.unparsed`.
    static func parseRefinement(_ raw: String, validLineRefs: [Int]) -> PermissiveRefinementResult {
        let trimmed = raw.trimmingCharacters(in: .whitespacesAndNewlines)
        guard !trimmed.isEmpty else { return .unparsed }

        let firstNonEmptyLine: String? = trimmed
            .split(whereSeparator: { $0.isNewline })
            .first(where: { !$0.trimmingCharacters(in: .whitespaces).isEmpty })
            .map { String($0).trimmingCharacters(in: .whitespaces) }
        guard let line = firstNonEmptyLine, !line.isEmpty else { return .unparsed }

        let upper = line.uppercased()
        if upper == "NO_AD" { return .noAd }
        if upper == "UNCERTAIN" { return .unparsed }
        guard upper.hasPrefix("AD ") else { return .unparsed }

        let body = String(line.dropFirst(3))
        let rawPairs = parseAdPairs(body)
        guard !rawPairs.isEmpty else { return .unparsed }

        // Clamp each pair to the window line refs. The window's line
        // refs are not necessarily a contiguous integer range (e.g.
        // [4, 5, 7, 8] if a planner gap exists), so clamping uses the
        // sorted set's min/max as the inclusive window bounds AND drops
        // pairs whose intersection with the valid set is empty.
        let validSorted = validLineRefs.sorted()
        guard let lo = validSorted.first, let hi = validSorted.last else {
            return .unparsed
        }
        let validSet = Set(validLineRefs)

        var clamped: [RefinementSpanPair] = []
        clamped.reserveCapacity(rawPairs.count)
        for (a, b) in rawPairs {
            let pairLo = min(a, b)
            let pairHi = max(a, b)
            // No overlap with the window at all → drop.
            if pairHi < lo || pairLo > hi { continue }
            let clampedLo = max(pairLo, lo)
            let clampedHi = min(pairHi, hi)
            // Walk the clamped range and find the tightest sub-range
            // whose endpoints both exist in the window. Sparse-window
            // case: the FM may name endpoints that fall in a planner
            // gap; we snap each end inward to the nearest valid ref.
            guard let snappedLo = validSet.contains(clampedLo)
                ? clampedLo
                : (clampedLo...clampedHi).first(where: { validSet.contains($0) }),
                  let snappedHi = validSet.contains(clampedHi)
                ? clampedHi
                : (clampedLo...clampedHi).reversed().first(where: { validSet.contains($0) }),
                  snappedLo <= snappedHi
            else {
                continue
            }
            clamped.append(RefinementSpanPair(firstLineRef: snappedLo, lastLineRef: snappedHi))
        }

        guard !clamped.isEmpty else { return .unparsed }
        return .spans(clamped)
    }

    /// Pull `(start, end)` integer pairs out of an `L<digits>-L<digits>`
    /// (comma-separated, optional whitespace) body. Returns an empty
    /// array if no concrete numeric pairs were found, which is the
    /// signal for "template parroting / garbage".
    private static func parseAdPairs(_ body: String) -> [(Int, Int)] {
        // Match L<digits>-L<digits>, case-insensitive on the L.
        // The literal `<` and `>` are deliberately NOT in the pattern,
        // so a template literal like `L<start>-L<end>` will not match
        // (because `<` is not a digit and not part of the alternation).
        let pattern = #"[Ll](\d+)\s*-\s*[Ll](\d+)"#
        guard let regex = try? NSRegularExpression(pattern: pattern, options: []) else {
            return []
        }
        let nsBody = body as NSString
        let matches = regex.matches(in: body, options: [], range: NSRange(location: 0, length: nsBody.length))
        var pairs: [(Int, Int)] = []
        for match in matches {
            guard match.numberOfRanges == 3 else { continue }
            let startRange = match.range(at: 1)
            let endRange = match.range(at: 2)
            guard startRange.location != NSNotFound,
                  endRange.location != NSNotFound,
                  let start = Int(nsBody.substring(with: startRange)),
                  let end = Int(nsBody.substring(with: endRange)) else {
                continue
            }
            pairs.append((start, end))
        }
        return pairs
    }
}

@available(iOS 26.0, *)
actor PermissiveAdClassifier {

    #if canImport(FoundationModels)
    private let model: SystemLanguageModel
    #endif

    private let logger: Logger

    init(logger: Logger = Logger(subsystem: "com.playhead", category: "PermissiveAdClassifier")) {
        self.logger = logger
        #if canImport(FoundationModels)
        self.model = SystemLanguageModel(guardrails: .permissiveContentTransformations)
        #endif
    }

    /// Classify a window of transcript segments through the permissive
    /// path. The result type matches the existing `coarsePassA`
    /// classifier so call sites can be polymorphic.
    func classify(window segments: [AdTranscriptSegment]) async -> CoarseScreeningSchema {
        let lineRefs = segments.map(\.segmentIndex)
        let prompt = PermissiveAdGrammar.buildPrompt(for: segments)

        #if canImport(FoundationModels)
        let session = LanguageModelSession(model: model)
        do {
            let response = try await session.respond(
                to: prompt,
                options: GenerationOptions(sampling: .greedy)
            )
            let raw = response.content.trimmingCharacters(in: .whitespacesAndNewlines)
            return PermissiveAdGrammar.parse(raw, validLineRefs: lineRefs)
        } catch let error as LanguageModelSession.GenerationError {
            // Apple's documented refusal / failure path. We treat all
            // four cases the same way: return `.uncertain` and let the
            // downstream pipeline decide what to do. The graceful
            // pattern matches `FoundationModelClassifier.coarsePassA`'s
            // tolerated-failure path.
            switch error {
            case .refusal:
                logger.debug("permissive_classifier_refused window=\(lineRefs.count, privacy: .public) segments")
            case .decodingFailure:
                logger.debug("permissive_classifier_decoding_failure window=\(lineRefs.count, privacy: .public) segments")
            case .exceededContextWindowSize:
                logger.debug("permissive_classifier_exceeded_context_window window=\(lineRefs.count, privacy: .public) segments")
            default:
                logger.debug("permissive_classifier_generation_error window=\(lineRefs.count, privacy: .public) segments error=\(String(describing: error), privacy: .public)")
            }
            return CoarseScreeningSchema(disposition: .uncertain, support: nil)
        } catch {
            logger.debug("permissive_classifier_unexpected_error window=\(lineRefs.count, privacy: .public) segments error=\(error.localizedDescription, privacy: .public)")
            return CoarseScreeningSchema(disposition: .uncertain, support: nil)
        }
        #else
        // No FoundationModels framework available (host or non-iOS-26
        // simulator build). Fall back to `.uncertain` so downstream
        // code path remains unchanged. The parser-only unit tests
        // exercise the parsing surface directly via the static helper.
        _ = prompt
        _ = lineRefs
        return CoarseScreeningSchema(disposition: .uncertain, support: nil)
        #endif
    }

    /// bd-1en Phase 2: refinement-pass entry point. Mirrors `classify`
    /// but uses the refinement prompt grammar and returns parsed
    /// `(firstLineRef, lastLineRef)` pairs (or `.noAd` / `.unparsed`).
    /// Same per-call session pattern, same greedy sampling, same
    /// graceful failure handling — any catastrophic failure collapses
    /// to `.unparsed` so the runner can fall back to the full coarse
    /// window rather than losing the ad outright.
    func refine(window segments: [AdTranscriptSegment]) async -> PermissiveRefinementResult {
        let lineRefs = segments.map(\.segmentIndex)
        let prompt = PermissiveAdGrammar.buildRefinementPrompt(for: segments)

        #if canImport(FoundationModels)
        let session = LanguageModelSession(model: model)
        do {
            let response = try await session.respond(
                to: prompt,
                options: GenerationOptions(sampling: .greedy)
            )
            let raw = response.content.trimmingCharacters(in: .whitespacesAndNewlines)
            return PermissiveAdGrammar.parseRefinement(raw, validLineRefs: lineRefs)
        } catch let error as LanguageModelSession.GenerationError {
            switch error {
            case .refusal:
                logger.debug("permissive_refinement_refused window=\(lineRefs.count, privacy: .public) segments")
            case .decodingFailure:
                logger.debug("permissive_refinement_decoding_failure window=\(lineRefs.count, privacy: .public) segments")
            case .exceededContextWindowSize:
                logger.debug("permissive_refinement_exceeded_context_window window=\(lineRefs.count, privacy: .public) segments")
            default:
                logger.debug("permissive_refinement_generation_error window=\(lineRefs.count, privacy: .public) segments error=\(String(describing: error), privacy: .public)")
            }
            return .unparsed
        } catch {
            logger.debug("permissive_refinement_unexpected_error window=\(lineRefs.count, privacy: .public) segments error=\(error.localizedDescription, privacy: .public)")
            return .unparsed
        }
        #else
        _ = prompt
        _ = lineRefs
        return .unparsed
        #endif
    }

}
