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
/// - `.spans` → one `RefinedAdSpan` per pair (anchorless by design;
///              the permissive path omits the evidence catalog).
/// - `.noAd`  → drop the window. The focused refinement contradicted
///              the coarse verdict; trust the more focused signal.
///
/// Cycle 2 H5 (Cycle 2): the previous `.unparsed` case has been
/// removed. Parser failures and adversarial integer ranges now throw
/// `PermissiveClassificationError.failed(reason: .permissiveDecodingFailure)`
/// out of `PermissiveAdClassifier.refine` instead of collapsing to a
/// rough full-window fallback span. The full-window fallback was
/// strictly worse UX than re-queueing the window: a misleadingly-precise
/// `.rough` span widened the ad-skip to the entire planner window even
/// when only one line was actually an ad.
enum PermissiveRefinementResult: Sendable, Equatable {
    case spans([RefinementSpanPair])
    case noAd
}

extension PermissiveRefinementResult: CustomStringConvertible {
    var description: String {
        switch self {
        case .spans: return "spans"
        case .noAd: return "noAd"
        }
    }
}

extension PermissiveRefinementResult {
    /// Convert this result into the `[RefinedAdSpan]` shape the runner
    /// persists. Bypasses `FoundationModelClassifier.sanitize` because
    /// that path rejects anchorless spans outright.
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
                    lastSegment: lastSegment
                )
            }
        }
    }

    /// Defaults: `.paid` + `.thirdParty` because the router only fires
    /// on coarse-confirmed sponsor-shaped pharma content. `.strong`
    /// because coarse already said containsAd. `memoryWriteEligible`
    /// is always false — anchorless spans never write to sponsor memory.
    /// Cycle 2 H4: `ownershipInferenceWasSuppressed` is always true on
    /// the permissive path — the FM never inferred these classification
    /// dimensions, the runner is hardcoding them.
    private static func makeAnchorlessSpan(
        firstLineRef: Int,
        lastLineRef: Int,
        firstSegment: AdTranscriptSegment,
        lastSegment: AdTranscriptSegment
    ) -> RefinedAdSpan {
        RefinedAdSpan(
            commercialIntent: .paid,
            ownership: .thirdParty,
            firstLineRef: firstLineRef,
            lastLineRef: lastLineRef,
            firstAtomOrdinal: firstSegment.firstAtomOrdinal,
            lastAtomOrdinal: lastSegment.lastAtomOrdinal,
            certainty: .strong,
            boundaryPrecision: .usable,
            resolvedEvidenceAnchors: [],
            memoryWriteEligible: false,
            alternativeExplanation: .unknown,
            reasonTags: [],
            ownershipInferenceWasSuppressed: true
        )
    }
}

/// A `(firstLineRef, lastLineRef)` pair extracted from a permissive
/// refinement response. Each pair becomes one `RefinedAdSpan`. Inclusive
/// on both ends.
struct RefinementSpanPair: Sendable, Equatable, Hashable {
    let firstLineRef: Int
    let lastLineRef: Int

    /// Cycle 2 Rev2-M2: enforce the inclusive-pair invariant at
    /// construction. A pair where `firstLineRef > lastLineRef` is a
    /// programmer error — the parser swaps mis-ordered model output
    /// before constructing pairs and downstream consumers (refinedSpans)
    /// assume well-ordered ranges.
    init(firstLineRef: Int, lastLineRef: Int) {
        precondition(
            firstLineRef <= lastLineRef,
            "RefinementSpanPair.init invariant: firstLineRef (\(firstLineRef)) must be <= lastLineRef (\(lastLineRef))"
        )
        self.firstLineRef = firstLineRef
        self.lastLineRef = lastLineRef
    }
}

/// Cycle 2 C2: errors thrown by `PermissiveAdClassifier.classify` and
/// `.refine` when the underlying FM call fails in a way the runner can
/// log and route through `failedWindowStatuses` instead of swallowing
/// as an `.uncertain` success row.
///
/// The runner catches `PermissiveClassificationError.failed(reason:)`
/// and increments per-reason counters that are emitted at run completion.
/// Refusal failures do NOT trigger a same-pass retry — the router
/// already guarantees this window would refuse the standard `@Generable`
/// path too. They are picked up next capability transition by the
/// shadow retry observer (Agent C handles the observer wake side).
public enum PermissiveClassificationError: Error, Equatable, Sendable {
    public enum Reason: String, Sendable, Equatable {
        case permissiveRefusal
        case permissiveDecodingFailure
        case permissiveContextOverflow
    }

    case failed(reason: Reason, underlyingDescription: String)

    public var reason: Reason {
        switch self {
        case let .failed(reason, _):
            return reason
        }
    }
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
    /// Cycle 2 C2/H6: throwing wrapper around `parse` that converts a
    /// non-actionable disposition into a thrown
    /// `PermissiveClassificationError`. Used by the actor's `classify`
    /// method so smart-shrink retries observe the same failure shape
    /// the runner sees.
    static func parseClassify(_ raw: String, validLineRefs: [Int]) throws -> CoarseScreeningSchema {
        // The existing `parse` returns `.uncertain` on garbage. We
        // intentionally do NOT throw on `.uncertain` here — uncertain
        // is a legitimate coarse outcome, not a failure mode.
        return parse(raw, validLineRefs: validLineRefs)
    }

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
    ///
    /// Cycle 2 H3: optionally surfaces three plan-derived hints in the
    /// prompt:
    ///
    ///   - `focusLineRefs` — the line refs the planner marked as the
    ///     highest-evidence concentration. Mirror of the standard
    ///     refinement path's `focusLineRefs` plumbing.
    ///   - `focusClusters` — contiguous line-ref clusters the planner
    ///     identified as belonging to a single ad read. Hint to the
    ///     model that these line refs probably belong together.
    ///   - `maximumSpans` — bounded by the planner; the model is told
    ///     not to return more than this many spans (the parser also
    ///     enforces it as a hard cap on output).
    ///
    /// Empty arrays / `Int.max` skip emission so existing call sites
    /// produce byte-identical prompts.
    static func buildRefinementPrompt(
        for segments: [AdTranscriptSegment],
        focusLineRefs: [Int] = [],
        focusClusters: [[Int]] = [],
        maximumSpans: Int = Int.max
    ) -> String {
        let lineRefs = segments.map(\.segmentIndex)
        let lineRefList = lineRefs.map { "L\($0)" }.joined(separator: ", ")
        let transcriptBody = segments
            .map { "L\($0.segmentIndex)> \"\($0.text)\"" }
            .joined(separator: "\n")

        var hintLines: [String] = []
        if !focusLineRefs.isEmpty {
            let focusList = focusLineRefs.map { "L\($0)" }.joined(separator: ", ")
            hintLines.append("Focus your refinement on these line refs first: \(focusList).")
        }
        if !focusClusters.isEmpty {
            let clusterDescriptions = focusClusters
                .filter { !$0.isEmpty }
                .map { cluster in
                    "[" + cluster.map { "L\($0)" }.joined(separator: ", ") + "]"
                }
                .joined(separator: " ")
            if !clusterDescriptions.isEmpty {
                hintLines.append("These clusters probably belong to the same ad read: \(clusterDescriptions).")
            }
        }
        if maximumSpans != Int.max {
            hintLines.append("Return at most \(maximumSpans) span(s).")
        }
        let hintBlock = hintLines.isEmpty ? "" : "\n\n" + hintLines.joined(separator: "\n")

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
          Transcript with L10, L11, L12 → if L10 and L12 are unrelated ads, output "AD L10-L10,L12-L12"\(hintBlock)

        Now refine this transcript:

        \(transcriptBody)
        """
    }

    /// Cycle 2 H3: hard-cap the parsed refinement result to
    /// `maximumSpans` and prioritize spans that overlap `focusLineRefs`.
    /// Spans without focus overlap come last and are dropped first
    /// during truncation; ties are broken by span size (denser
    /// concentrations win) and by `firstLineRef` (deterministic).
    static func applyFocusAndCap(
        to result: PermissiveRefinementResult,
        focusLineRefs: [Int],
        maximumSpans: Int
    ) -> PermissiveRefinementResult {
        guard case let .spans(pairs) = result else { return result }
        guard maximumSpans != Int.max && pairs.count > maximumSpans else {
            return result
        }
        let focusSet = Set(focusLineRefs)

        // Score each pair by overlap with focusLineRefs (descending),
        // then by span size (ascending — tighter spans win), then by
        // firstLineRef (ascending — deterministic tiebreaker).
        let sorted = pairs.enumerated().sorted { lhs, rhs in
            let lhsOverlap = focusOverlap(pair: lhs.element, focusSet: focusSet)
            let rhsOverlap = focusOverlap(pair: rhs.element, focusSet: focusSet)
            if lhsOverlap != rhsOverlap { return lhsOverlap > rhsOverlap }
            let lhsSize = lhs.element.lastLineRef - lhs.element.firstLineRef + 1
            let rhsSize = rhs.element.lastLineRef - rhs.element.firstLineRef + 1
            if lhsSize != rhsSize { return lhsSize < rhsSize }
            return lhs.element.firstLineRef < rhs.element.firstLineRef
        }
        let truncated = sorted.prefix(maximumSpans).map(\.element)
        // Restore the original first-line-ref order so downstream
        // span comparisons (and the persistence layer) see a stable
        // ordering rather than the priority order.
        let restored = truncated.sorted { $0.firstLineRef < $1.firstLineRef }
        return .spans(restored)
    }

    private static func focusOverlap(
        pair: RefinementSpanPair,
        focusSet: Set<Int>
    ) -> Int {
        guard !focusSet.isEmpty else { return 0 }
        var count = 0
        for ref in pair.firstLineRef...pair.lastLineRef where focusSet.contains(ref) {
            count += 1
        }
        return count
    }

    /// Refinement variant of `parse`. Preserves pair structure (so
    /// non-contiguous spans become separate `RefinedAdSpan`s), clamps
    /// each pair to `validLineRefs`, and snaps endpoints inward when
    /// the window has planner gaps.
    ///
    /// Cycle 2 H5: throws `PermissiveClassificationError.failed(reason:
    /// .permissiveDecodingFailure)` on garbage / template-parroting /
    /// refusal-shaped output instead of returning `.unparsed`. The
    /// runner catches this and re-queues the window for next pass
    /// rather than emitting a misleadingly-precise full-window span.
    ///
    /// Cycle 2 Rev2-M3: integer-range expansion is hard-capped. If a
    /// `(start, end)` pair would expand to more than 10,000 line refs
    /// (adversarial input — the FM hallucinates a billion-line range),
    /// the parser treats it as a decoding failure.
    static func parseRefinement(_ raw: String, validLineRefs: [Int]) throws -> PermissiveRefinementResult {
        let trimmed = raw.trimmingCharacters(in: .whitespacesAndNewlines)
        guard !trimmed.isEmpty else {
            throw PermissiveClassificationError.failed(
                reason: .permissiveDecodingFailure,
                underlyingDescription: "empty response"
            )
        }

        let firstNonEmptyLine: String? = trimmed
            .split(whereSeparator: { $0.isNewline })
            .first(where: { !$0.trimmingCharacters(in: .whitespaces).isEmpty })
            .map { String($0).trimmingCharacters(in: .whitespaces) }
        guard let line = firstNonEmptyLine, !line.isEmpty else {
            throw PermissiveClassificationError.failed(
                reason: .permissiveDecodingFailure,
                underlyingDescription: "no non-empty first line"
            )
        }

        let upper = line.uppercased()
        if upper == "NO_AD" { return .noAd }
        if upper == "UNCERTAIN" {
            throw PermissiveClassificationError.failed(
                reason: .permissiveDecodingFailure,
                underlyingDescription: "model returned UNCERTAIN — no actionable refinement"
            )
        }
        guard upper.hasPrefix("AD ") else {
            throw PermissiveClassificationError.failed(
                reason: .permissiveDecodingFailure,
                underlyingDescription: "first line lacks AD prefix: \(line.prefix(64))"
            )
        }

        let body = String(line.dropFirst(3))
        let rawPairs = parseAdPairs(body)
        guard !rawPairs.isEmpty else {
            throw PermissiveClassificationError.failed(
                reason: .permissiveDecodingFailure,
                underlyingDescription: "no parseable L<n>-L<m> pairs in body: \(body.prefix(64))"
            )
        }

        // Rev2-M3: cap range expansion. The FM occasionally hallucinates
        // huge integer ranges (`AD L0-L999999`); expanding those is a
        // CPU and memory hazard. Treat any single pair larger than the
        // cap as a decoding failure.
        for (a, b) in rawPairs {
            let lo = min(a, b)
            let hi = max(a, b)
            if hi - lo + 1 > Self.maximumRangeExpansion {
                throw PermissiveClassificationError.failed(
                    reason: .permissiveDecodingFailure,
                    underlyingDescription: "pair (\(lo), \(hi)) expands to more than \(Self.maximumRangeExpansion) line refs"
                )
            }
        }

        // Clamp each pair to the window line refs. The window's line
        // refs are not necessarily a contiguous integer range (e.g.
        // [4, 5, 7, 8] if a planner gap exists), so clamping uses the
        // sorted set's min/max as the inclusive window bounds AND drops
        // pairs whose intersection with the valid set is empty.
        let validSorted = validLineRefs.sorted()
        guard let lo = validSorted.first, let hi = validSorted.last else {
            throw PermissiveClassificationError.failed(
                reason: .permissiveDecodingFailure,
                underlyingDescription: "window has no valid line refs"
            )
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

        guard !clamped.isEmpty else {
            throw PermissiveClassificationError.failed(
                reason: .permissiveDecodingFailure,
                underlyingDescription: "no pairs survived clamp/snap to valid window line refs"
            )
        }
        return .spans(clamped)
    }

    /// Cycle 2 Rev2-M3: hard cap on integer range expansion in
    /// `parseRefinement`. Larger ranges throw `.permissiveDecodingFailure`.
    static let maximumRangeExpansion: Int = 10_000

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

    #if DEBUG
    /// Cycle 4 H-2 / M-3: test-only fault injection. When set, the
    /// classify / refine methods short-circuit and throw the returned
    /// `PermissiveClassificationError` BEFORE touching the real
    /// `SystemLanguageModel`. Nil in production. The hook is confined
    /// to DEBUG so the production code path is byte-identical to the
    /// pre-hook build.
    ///
    /// The closure receives the reason "classify" or "refine" so tests
    /// can distinguish which path was invoked on a single mock.
    var faultInjectionForTesting: ((_ path: String) -> PermissiveClassificationError?)?
    #endif

    init(logger: Logger = Logger(subsystem: "com.playhead", category: "PermissiveAdClassifier")) {
        self.logger = logger
        #if canImport(FoundationModels)
        self.model = SystemLanguageModel(guardrails: .permissiveContentTransformations)
        #endif
    }

    #if DEBUG
    /// Cycle 4 H-2: install a fault-injection closure. Tests call this
    /// before the runner dispatches so `classify`/`refine` throw the
    /// desired `PermissiveClassificationError` variant without booting
    /// the real FoundationModels session.
    func installFaultInjectionForTesting(
        _ closure: ((_ path: String) -> PermissiveClassificationError?)?
    ) {
        self.faultInjectionForTesting = closure
    }
    #endif

    /// Classify a window of transcript segments through the permissive
    /// path. The result type matches the existing `coarsePassA`
    /// classifier so call sites can be polymorphic.
    ///
    /// Cycle 2 C2: throws `PermissiveClassificationError.failed(reason:)`
    /// on refusal / decoding failure / context overflow / unknown
    /// `GenerationError` instead of swallowing them as `.uncertain`.
    /// The runner catches these, appends the per-window status to
    /// `failedWindowStatuses`, and increments per-reason telemetry
    /// counters that are emitted at run completion.
    ///
    /// Cycle 2 H6: context-overflow on the permissive coarse path now
    /// runs through `runSmartShrinkRetry`, which iteratively shrinks
    /// the prompt by halving the segment count and retrying — mirroring
    /// the standard coarse-path smart-shrink behavior. After
    /// `coarseSmartShrinkMaxIterations` attempts the loop gives up
    /// and rethrows `.permissiveContextOverflow`.
    func classify(window segments: [AdTranscriptSegment]) async throws -> CoarseScreeningSchema {
        let lineRefs = segments.map(\.segmentIndex)

        #if DEBUG
        if let fault = faultInjectionForTesting?("classify") {
            throw fault
        }
        #endif

        #if canImport(FoundationModels)
        do {
            let prompt = PermissiveAdGrammar.buildPrompt(for: segments)
            let raw = try await respond(to: prompt)
            return try PermissiveAdGrammar.parseClassify(raw, validLineRefs: lineRefs)
        } catch is CancellationError {
            // Cycle 4 M-1: cooperative cancellation must propagate
            // untouched. The prior catch-all below would have mapped
            // this to `.permissiveDecodingFailure`, which is incorrect
            // — a cancelled task is not a model failure.
            throw CancellationError()
        } catch let error as LanguageModelSession.GenerationError {
            // Cycle 2 C2 + H6: route the documented failure cases
            // through `PermissiveClassificationError` so the runner can
            // surface them in `failedWindowStatuses`.
            switch error {
            case .refusal:
                logger.debug("permissive_classifier_refused window=\(lineRefs.count, privacy: .public) segments error=\(String(describing: error), privacy: .private)")
                throw PermissiveClassificationError.failed(
                    reason: .permissiveRefusal,
                    underlyingDescription: String(describing: error)
                )
            case .decodingFailure:
                logger.debug("permissive_classifier_decoding_failure window=\(lineRefs.count, privacy: .public) segments error=\(String(describing: error), privacy: .private)")
                throw PermissiveClassificationError.failed(
                    reason: .permissiveDecodingFailure,
                    underlyingDescription: String(describing: error)
                )
            case .exceededContextWindowSize:
                // H6: invoke smart-shrink retry helper before reporting
                // terminal overflow. The helper halves the segment
                // count up to `coarseSmartShrinkMaxIterations` times.
                return try await runClassifySmartShrinkRetry(
                    initialSegments: segments,
                    initialError: error
                )
            default:
                logger.debug("permissive_classifier_generation_error window=\(lineRefs.count, privacy: .public) segments error=\(String(describing: error), privacy: .private)")
                throw PermissiveClassificationError.failed(
                    reason: .permissiveDecodingFailure,
                    underlyingDescription: String(describing: error)
                )
            }
        } catch let error as PermissiveClassificationError {
            // Re-throw a parser-derived classification error untouched.
            throw error
        } catch {
            // Rev2-L4: log the unexpected-error description with .private
            // privacy in case it ever contains transcript content.
            logger.debug("permissive_classifier_unexpected_error window=\(lineRefs.count, privacy: .public) segments error=\(error.localizedDescription, privacy: .private)")
            throw PermissiveClassificationError.failed(
                reason: .permissiveDecodingFailure,
                underlyingDescription: error.localizedDescription
            )
        }
        #else
        // No FoundationModels framework available (host or non-iOS-26
        // simulator build). The parser-only unit tests exercise the
        // parsing surface directly via the static helper; production
        // dispatch never reaches this branch because the @available
        // gate forbids it.
        _ = lineRefs
        return CoarseScreeningSchema(disposition: .uncertain, support: nil)
        #endif
    }

    /// bd-1en Phase 2: refinement-pass entry point. Mirrors `classify`
    /// but uses the refinement prompt grammar and returns parsed
    /// `(firstLineRef, lastLineRef)` pairs (or `.noAd`).
    ///
    /// Cycle 2 C2: throws `PermissiveClassificationError.failed(reason:)`
    /// on refusal / decoding failure / context overflow.
    ///
    /// Cycle 2 H3: now plumbs `focusLineRefs`, `focusClusters`, and
    /// `maximumSpans` from the calling refinement plan into the prompt
    /// (as hints) and into the parsed-result truncation step (hard
    /// cap on returned spans, prioritized by overlap with focusLineRefs
    /// then by anchor density). The standard refinement path already
    /// honors all three; honoring them on the permissive path keeps
    /// the two paths producing within-±1 line-ref equivalent output
    /// for the same window.
    func refine(
        window segments: [AdTranscriptSegment],
        focusLineRefs: [Int] = [],
        focusClusters: [[Int]] = [],
        maximumSpans: Int = Int.max
    ) async throws -> PermissiveRefinementResult {
        let lineRefs = segments.map(\.segmentIndex)

        #if DEBUG
        if let fault = faultInjectionForTesting?("refine") {
            throw fault
        }
        #endif

        #if canImport(FoundationModels)
        do {
            let prompt = PermissiveAdGrammar.buildRefinementPrompt(
                for: segments,
                focusLineRefs: focusLineRefs,
                focusClusters: focusClusters,
                maximumSpans: maximumSpans
            )
            let raw = try await respond(to: prompt)
            let parsed = try PermissiveAdGrammar.parseRefinement(raw, validLineRefs: lineRefs)
            return PermissiveAdGrammar.applyFocusAndCap(
                to: parsed,
                focusLineRefs: focusLineRefs,
                maximumSpans: maximumSpans
            )
        } catch is CancellationError {
            // Cycle 4 M-1: propagate cooperative cancellation untouched.
            throw CancellationError()
        } catch let error as LanguageModelSession.GenerationError {
            switch error {
            case .refusal:
                logger.debug("permissive_refinement_refused window=\(lineRefs.count, privacy: .public) segments error=\(String(describing: error), privacy: .private)")
                throw PermissiveClassificationError.failed(
                    reason: .permissiveRefusal,
                    underlyingDescription: String(describing: error)
                )
            case .decodingFailure:
                logger.debug("permissive_refinement_decoding_failure window=\(lineRefs.count, privacy: .public) segments error=\(String(describing: error), privacy: .private)")
                throw PermissiveClassificationError.failed(
                    reason: .permissiveDecodingFailure,
                    underlyingDescription: String(describing: error)
                )
            case .exceededContextWindowSize:
                logger.debug("permissive_refinement_exceeded_context_window window=\(lineRefs.count, privacy: .public) segments error=\(String(describing: error), privacy: .private)")
                throw PermissiveClassificationError.failed(
                    reason: .permissiveContextOverflow,
                    underlyingDescription: String(describing: error)
                )
            default:
                logger.debug("permissive_refinement_generation_error window=\(lineRefs.count, privacy: .public) segments error=\(String(describing: error), privacy: .private)")
                throw PermissiveClassificationError.failed(
                    reason: .permissiveDecodingFailure,
                    underlyingDescription: String(describing: error)
                )
            }
        } catch let error as PermissiveClassificationError {
            throw error
        } catch {
            // Rev2-L4: log the unexpected-error description with .private
            // privacy in case it ever contains transcript content.
            logger.debug("permissive_refinement_unexpected_error window=\(lineRefs.count, privacy: .public) segments error=\(error.localizedDescription, privacy: .private)")
            throw PermissiveClassificationError.failed(
                reason: .permissiveDecodingFailure,
                underlyingDescription: error.localizedDescription
            )
        }
        #else
        _ = lineRefs
        _ = focusLineRefs
        _ = focusClusters
        _ = maximumSpans
        return .noAd
        #endif
    }

    #if canImport(FoundationModels)
    /// Issue one greedy-sampled prompt against a fresh per-call session
    /// and return the trimmed response content. Factored out so the
    /// classify and refine paths share the same session lifecycle.
    private func respond(to prompt: String) async throws -> String {
        let session = LanguageModelSession(model: model)
        let response = try await session.respond(
            to: prompt,
            options: GenerationOptions(sampling: .greedy)
        )
        return response.content.trimmingCharacters(in: .whitespacesAndNewlines)
    }

    /// Cycle 2 H6: shared smart-shrink retry helper for the permissive
    /// coarse path. Mirrors the standard `runCoarseSmartShrinkLoop`
    /// shape: each iteration halves the segment count and retries
    /// until either a successful response is parsed or
    /// `coarseSmartShrinkMaxIterations` attempts have been exhausted.
    /// After exhaustion the helper rethrows
    /// `PermissiveClassificationError.failed(reason: .permissiveContextOverflow)`
    /// so the runner can surface a terminal overflow in
    /// `failedWindowStatuses`.
    private func runClassifySmartShrinkRetry(
        initialSegments: [AdTranscriptSegment],
        initialError: LanguageModelSession.GenerationError
    ) async throws -> CoarseScreeningSchema {
        // Cycle 4 M-3: the loop body is factored into
        // `PermissiveAdClassifier.smartShrinkClassify` so tests can
        // inject a fail-then-succeed `respond` closure without having
        // to boot a real `LanguageModelSession`. The actor method here
        // is a thin adapter that forwards the production
        // `respond(to:)` call site.
        return try await Self.smartShrinkClassify(
            initialSegments: initialSegments,
            initialError: initialError,
            maxIterations: Self.coarseSmartShrinkMaxIterations,
            respond: { prompt in try await self.respond(to: prompt) }
        )
    }

    /// Maximum number of smart-shrink iterations on the permissive
    /// coarse path. Matches `FoundationModelClassifier.coarseSmartShrinkMaxIterations`
    /// in spirit; defined locally so the actor doesn't need a static
    /// import.
    static let coarseSmartShrinkMaxIterations: Int = 3

    /// Cycle 4 M-3: testable extraction of the smart-shrink retry loop.
    /// Each iteration halves the current segment count, rebuilds the
    /// permissive prompt, and calls the injected `respond` closure. On
    /// success the parsed `CoarseScreeningSchema` is returned. On
    /// repeated `.exceededContextWindowSize` the loop continues; any
    /// other thrown error promotes to
    /// `PermissiveClassificationError.failed(.permissiveDecodingFailure,
    /// …)`. After `maxIterations` attempts the helper throws
    /// `.permissiveContextOverflow`.
    ///
    /// The helper is `static` and consumes a closure so unit tests can
    /// construct a fail-then-succeed stub without touching the actor's
    /// private `respond(to:)` path.
    static func smartShrinkClassify(
        initialSegments: [AdTranscriptSegment],
        initialError: LanguageModelSession.GenerationError,
        maxIterations: Int,
        respond: @Sendable (String) async throws -> String
    ) async throws -> CoarseScreeningSchema {
        var current = initialSegments
        var lastError: Error = initialError
        for _ in 0..<maxIterations {
            let target = max(1, current.count / 2)
            guard target < current.count else { break }
            current = Array(current.prefix(target))
            do {
                let prompt = PermissiveAdGrammar.buildPrompt(for: current)
                let raw = try await respond(prompt)
                return try PermissiveAdGrammar.parseClassify(
                    raw,
                    validLineRefs: current.map(\.segmentIndex)
                )
            } catch is CancellationError {
                // Cycle 4 M-1: cooperative cancellation must propagate
                // through the shrink loop untouched.
                throw CancellationError()
            } catch let error as LanguageModelSession.GenerationError {
                if case .exceededContextWindowSize = error {
                    lastError = error
                    continue
                }
                throw PermissiveClassificationError.failed(
                    reason: .permissiveDecodingFailure,
                    underlyingDescription: String(describing: error)
                )
            }
        }
        throw PermissiveClassificationError.failed(
            reason: .permissiveContextOverflow,
            underlyingDescription: String(describing: lastError)
        )
    }
    #endif

}
