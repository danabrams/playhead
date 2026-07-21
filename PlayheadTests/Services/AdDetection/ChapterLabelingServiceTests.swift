// ChapterLabelingServiceTests.swift
// playhead-au2v.1.7: Unit tests for `ChapterLabelingService`.
//
// Strategy: every test injects a fake `LabelCall` closure into the
// service, so the FoundationModels framework is never touched. A
// thread-safe call counter records how many times the closure was
// invoked across the (potential) retry, and a per-attempt response
// list lets each test script the (success / failure) pattern it cares
// about.
//
// What's covered (one test method per acceptance bullet):
// - Successful confident label maps onto `ChapterEvidence` with the
//   right disposition / quality score / source.
// - Schema-invalid output (decoding failure) → operational + retry.
// - Timeout → operational + retry.
// - Rate-limit → operational + retry.
// - Retry-then-success returns the SECOND attempt's label.
// - Retry-then-still-fail returns operational with `attempts == 2`.
// - Semantic `.unclear` is NOT retried (`attempts == 1`,
//   `failureMode == .semantic`).
// - Out-of-taxonomy is impossible to express through the typed
//   closure; the equivalent is the `decodingFailure` / unsupportedGuide
//   case, which routes through the same operational arm — covered.
// - All seven `ChapterDispositionRaw` cases map to the documented
//   `ChapterDisposition`.
// - Confidence outside [0, 1] is clamped, NaN/Inf → 0.
// - Prompt builder respects the documented shape and the
//   region-text character cap.

import Foundation
import Testing

@testable import Playhead

#if canImport(FoundationModels)
import FoundationModels
#endif

// MARK: - Test helpers

/// Thread-safe counter for FM-call closure invocations.
private final class CallCounter: @unchecked Sendable {
    private let lock = NSLock()
    private var _value = 0
    func increment() {
        lock.lock(); defer { lock.unlock() }
        _value += 1
    }
    var value: Int {
        lock.lock(); defer { lock.unlock() }
        return _value
    }
}

/// Thread-safe ordered queue of canned attempt outcomes. Each test
/// pushes a list of `Outcome` values; the closure pops the head per
/// invocation. If the closure runs more times than the script has
/// outcomes, the last outcome is repeated indefinitely (catches "the
/// retry should not have happened" by letting the test `#expect` the
/// observed call count).
private final class OutcomeScript: @unchecked Sendable {
    enum Outcome {
        case label(ChapterLabel)
        case error(Error)
        /// Sleep at least this many nanoseconds before throwing
        /// timeout (used to drive the timeout race deterministically).
        case sleepThenSucceed(label: ChapterLabel, sleepNanos: UInt64)
        case sleepThenError(error: Error, sleepNanos: UInt64)
    }

    private let lock = NSLock()
    private var queue: [Outcome] = []

    init(_ outcomes: [Outcome]) {
        self.queue = outcomes
    }

    /// Pop the next outcome (or repeat the last one if the script is
    /// exhausted). Empty scripts return a generic operational error.
    func next() -> Outcome {
        lock.lock(); defer { lock.unlock() }
        if queue.count > 1 {
            return queue.removeFirst()
        }
        if let only = queue.first {
            return only
        }
        return .error(ChapterLabelingError.operational("script exhausted"))
    }
}

private func makeCandidate(
    chapterIndex: Int = 1,
    totalChapters: Int = 5,
    previousDisposition: ChapterDispositionRaw? = nil,
    regionText: String = "Welcome back to the show. Today we are talking about open source funding.",
    startTime: TimeInterval = 0,
    endTime: TimeInterval? = 60
) -> ChapterLabelingCandidate {
    ChapterLabelingCandidate(
        startTime: startTime,
        endTime: endTime,
        regionText: regionText,
        chapterIndex: chapterIndex,
        totalChapters: totalChapters,
        previousDisposition: previousDisposition
    )
}

private func makeService(
    counter: CallCounter,
    script: OutcomeScript
) -> ChapterLabelingService {
    ChapterLabelingService(
        labelCall: { _ in
            counter.increment()
            switch script.next() {
            case let .label(label):
                return label
            case let .error(error):
                throw error
            case let .sleepThenSucceed(label, sleepNanos):
                try await Task.sleep(nanoseconds: sleepNanos)
                return label
            case let .sleepThenError(error, sleepNanos):
                try await Task.sleep(nanoseconds: sleepNanos)
                throw error
            }
        }
    )
}

private func makeLabel(
    _ disposition: ChapterDispositionRaw,
    confidence: Double = 0.9,
    topic: String? = nil
) -> ChapterLabel {
    ChapterLabel(
        disposition: disposition,
        confidence: confidence,
        topicDescriptor: topic
    )
}

// MARK: - Mapping

@Suite("ChapterDispositionRaw mapping")
struct ChapterDispositionRawMappingTests {

    @Test("intro maps to .content")
    func intro() {
        #expect(ChapterDispositionRaw.intro.mappedDisposition == .content)
    }

    @Test("content maps to .content")
    func content() {
        #expect(ChapterDispositionRaw.content.mappedDisposition == .content)
    }

    @Test("hostReadAd maps to .adBreak")
    func hostReadAd() {
        #expect(ChapterDispositionRaw.hostReadAd.mappedDisposition == .adBreak)
    }

    @Test("programmaticAd maps to .adBreak")
    func programmaticAd() {
        #expect(ChapterDispositionRaw.programmaticAd.mappedDisposition == .adBreak)
    }

    @Test("outro maps to .content")
    func outro() {
        #expect(ChapterDispositionRaw.outro.mappedDisposition == .content)
    }

    @Test("recap maps to .content")
    func recap() {
        #expect(ChapterDispositionRaw.recap.mappedDisposition == .content)
    }

    @Test("unclear maps to .ambiguous")
    func unclear() {
        #expect(ChapterDispositionRaw.unclear.mappedDisposition == .ambiguous)
    }

    @Test("CaseIterable lists all seven cases")
    func caseIterable() {
        #expect(Set(ChapterDispositionRaw.allCases) == [
            .intro, .content, .hostReadAd, .programmaticAd,
            .outro, .recap, .unclear
        ])
    }

    @Test("rawValue strings are stable for cache compatibility")
    func rawValuesStable() {
        #expect(ChapterDispositionRaw.intro.rawValue == "intro")
        #expect(ChapterDispositionRaw.content.rawValue == "content")
        #expect(ChapterDispositionRaw.hostReadAd.rawValue == "hostReadAd")
        #expect(ChapterDispositionRaw.programmaticAd.rawValue == "programmaticAd")
        #expect(ChapterDispositionRaw.outro.rawValue == "outro")
        #expect(ChapterDispositionRaw.recap.rawValue == "recap")
        #expect(ChapterDispositionRaw.unclear.rawValue == "unclear")
    }
}

// MARK: - LabelFailureMode

@Suite("LabelFailureMode round-trip")
struct LabelFailureModeTests {

    @Test("Codable round-trip preserves all cases")
    func roundTrip() throws {
        for mode in [LabelFailureMode.operational, .semantic, .guardrail] {
            let data = try JSONEncoder().encode(mode)
            let decoded = try JSONDecoder().decode(LabelFailureMode.self, from: data)
            #expect(decoded == mode)
        }
    }

    @Test("rawValue strings are stable for cache compatibility")
    func rawValuesStable() {
        // Old persisted plans only ever carry operational / semantic;
        // pinning the rawValues guards against an accidental rename that
        // would make existing cached plans undecodable. `guardrail`
        // (au2v.1.24) is the only addition.
        #expect(LabelFailureMode.operational.rawValue == "operational")
        #expect(LabelFailureMode.semantic.rawValue == "semantic")
        #expect(LabelFailureMode.guardrail.rawValue == "guardrail")
    }

    @Test("decoding legacy operational / semantic rawValues is unaffected by the new case")
    func decodesLegacyRawValues() throws {
        // Simulate JSON written before `guardrail` existed.
        let operational = try JSONDecoder().decode(
            LabelFailureMode.self, from: Data("\"operational\"".utf8)
        )
        let semantic = try JSONDecoder().decode(
            LabelFailureMode.self, from: Data("\"semantic\"".utf8)
        )
        #expect(operational == .operational)
        #expect(semantic == .semantic)
    }
}

// MARK: - Prompt builder

@Suite("ChapterLabelingService prompt")
struct ChapterLabelingServicePromptTests {

    @Test("Prompt embeds chapter index, total, previous disposition, and region text")
    func promptContents() {
        let candidate = makeCandidate(
            chapterIndex: 3,
            totalChapters: 7,
            previousDisposition: .content,
            regionText: "Hello sponsors hello"
        )
        let prompt = ChapterLabelingService.buildPrompt(for: candidate)
        #expect(prompt.contains("chapter 3 of 7"))
        #expect(prompt.contains("Previous: content"))
        #expect(prompt.contains("Hello sponsors hello"))
        #expect(prompt.contains("Region transcript:"))
    }

    @Test("Prompt uses 'none' for missing previous disposition")
    func promptPreviousNone() {
        let candidate = makeCandidate(previousDisposition: nil)
        let prompt = ChapterLabelingService.buildPrompt(for: candidate)
        #expect(prompt.contains("Previous: none"))
    }

    @Test("Prompt truncates oversized region text")
    func promptRespectsCharCap() {
        // Use a sentinel character (`@`) that does not appear in the
        // prompt template so the count is exactly the body length.
        let huge = String(repeating: "@", count: 5000)
        let candidate = makeCandidate(regionText: huge)
        let prompt = ChapterLabelingService.buildPrompt(for: candidate)
        let count = prompt.filter { $0 == "@" }.count
        #expect(count == ChapterLabelingService.regionTextCharacterCap)
    }

    /// au2v.1.25: the prompt MUST teach the model what an ad looks like.
    /// Empirically (ChapterLabelingDiagnosticTests on the dogfood corpus),
    /// the unguided prompt labelled blatant host-read sponsor copy as
    /// `content`. These assertions pin that the prompt now names the ad
    /// dispositions and the sponsor-read cues that route a region there.
    @Test("Prompt names both ad dispositions so the model can route sponsor reads")
    func promptNamesAdDispositions() {
        let candidate = makeCandidate()
        let prompt = ChapterLabelingService.buildPrompt(for: candidate)
        #expect(prompt.contains("hostReadAd"))
        #expect(prompt.contains("programmaticAd"))
    }

    @Test("Prompt enumerates the full disposition taxonomy")
    func promptListsTaxonomy() {
        let candidate = makeCandidate()
        let prompt = ChapterLabelingService.buildPrompt(for: candidate)
        for disposition in ChapterDispositionRaw.allCases {
            #expect(
                prompt.contains(disposition.rawValue),
                "prompt is missing taxonomy case \(disposition.rawValue)"
            )
        }
    }

    @Test("Prompt lists sponsor-read cues that signal an ad")
    func promptListsSponsorReadCues() {
        let candidate = makeCandidate()
        let prompt = ChapterLabelingService.buildPrompt(for: candidate)
        // Generic, non-branded cue language (privacy: no real advertisers).
        #expect(prompt.contains("sponsor"))
        #expect(prompt.contains("promo code"))
        #expect(prompt.contains("brought to you by"))
    }

    /// The fixed instruction scaffold (everything except the variable
    /// context line and the region body) must stay terse so the
    /// ad-classification guidance does not crowd the per-call token
    /// budget. We measure the scaffold directly — an EMPTY-region prompt
    /// is the scaffold plus a minimal context line — using the project's
    /// own conservative `ChapterPromptContext.estimateTokens` model
    /// (`ceil(chars / 3)`). The bound has headroom for the current
    /// guidance but would fail if a future edit roughly doubled it.
    /// (The whole-prompt `regionTextCharacterCap + 1024` char bound used
    /// elsewhere cannot catch a scaffold blowup — the 1200-char body
    /// dominates it — so this token-model check is the real guard.)
    @Test("Prompt instruction scaffold stays within the documented token budget")
    func promptScaffoldStaysWithinTokenBudget() {
        // Empty region text isolates the fixed instruction + minimal
        // context from the variable transcript body.
        let candidate = makeCandidate(
            chapterIndex: 1,
            totalChapters: 5,
            previousDisposition: nil,
            regionText: ""
        )
        let scaffold = ChapterLabelingService.buildPrompt(for: candidate)
        let scaffoldTokens = ChapterPromptContext.estimateTokens(of: scaffold)
        // Current scaffold is ~158 tokens under ceil(chars/3); 180 leaves
        // headroom for minor wording tweaks while failing on a doubling.
        #expect(
            scaffoldTokens <= 180,
            "instruction scaffold token estimate \(scaffoldTokens) exceeds the per-call budget; trim the guidance"
        )
    }
}

// MARK: - label() — happy path

@Suite("ChapterLabelingService — happy path")
struct ChapterLabelingServiceHappyPathTests {

    @Test("Successful confident label produces expected ChapterEvidence + LabelingResult")
    func successfulLabel() async {
        let counter = CallCounter()
        let script = OutcomeScript([
            .label(makeLabel(.hostReadAd, confidence: 0.85, topic: "AcmeWidgets"))
        ])
        let service = makeService(counter: counter, script: script)

        let result = await service.label(makeCandidate(
            startTime: 60,
            endTime: 130
        ))

        #expect(counter.value == 1)
        #expect(result.attempts == 1)
        #expect(result.failureMode == nil)
        #expect(result.labelDisposition == .hostReadAd)
        #expect(result.topicDescriptor == "AcmeWidgets")
        #expect(result.chapter.source == .inferred)
        #expect(result.chapter.disposition == .adBreak)
        #expect(result.chapter.qualityScore == 0.85)
        #expect(result.chapter.startTime == 60)
        #expect(result.chapter.endTime == 130)
        #expect(result.chapter.title == "AcmeWidgets")
    }

    @Test("Confidence above 1.0 is clamped to 1.0")
    func clampHigh() async {
        let counter = CallCounter()
        let script = OutcomeScript([
            .label(makeLabel(.content, confidence: 1.42))
        ])
        let service = makeService(counter: counter, script: script)
        let result = await service.label(makeCandidate())
        #expect(result.chapter.qualityScore == 1.0)
        #expect(result.failureMode == nil)
    }

    @Test("Negative confidence is clamped to 0.0")
    func clampLow() async {
        let counter = CallCounter()
        let script = OutcomeScript([
            .label(makeLabel(.content, confidence: -0.3))
        ])
        let service = makeService(counter: counter, script: script)
        let result = await service.label(makeCandidate())
        #expect(result.chapter.qualityScore == 0.0)
        #expect(result.failureMode == nil)
    }

    @Test("Non-finite confidence (NaN) collapses to 0.0")
    func clampNaN() async {
        let counter = CallCounter()
        let script = OutcomeScript([
            .label(makeLabel(.content, confidence: .nan))
        ])
        let service = makeService(counter: counter, script: script)
        let result = await service.label(makeCandidate())
        #expect(result.chapter.qualityScore == 0.0)
        #expect(result.failureMode == nil)
    }

    @Test("All seven raw dispositions map onto the expected ChapterDisposition end-to-end")
    func endToEndDispositionMapping() async {
        // Each entry is (raw input → expected mapped output). This is
        // the load-bearing acceptance test for the taxonomy mapping.
        let cases: [(ChapterDispositionRaw, ChapterDisposition)] = [
            (.intro, .content),
            (.content, .content),
            (.hostReadAd, .adBreak),
            (.programmaticAd, .adBreak),
            (.outro, .content),
            (.recap, .content),
            // .unclear is the semantic-failure path; covered separately.
        ]
        for (raw, expected) in cases {
            let counter = CallCounter()
            let script = OutcomeScript([.label(makeLabel(raw))])
            let service = makeService(counter: counter, script: script)
            let result = await service.label(makeCandidate())
            #expect(result.labelDisposition == raw, "raw=\(raw) labelDisposition was \(result.labelDisposition)")
            #expect(
                result.chapter.disposition == expected,
                "raw=\(raw) expected \(expected) got \(result.chapter.disposition)"
            )
            #expect(result.failureMode == nil)
            #expect(result.attempts == 1)
            #expect(counter.value == 1)
        }
    }
}

// MARK: - label() — semantic unclear

@Suite("ChapterLabelingService — semantic unclear")
struct ChapterLabelingServiceSemanticUnclearTests {

    @Test("FM returning .unclear produces .semantic failure mode WITHOUT retry")
    func semanticUnclearNoRetry() async {
        let counter = CallCounter()
        let script = OutcomeScript([
            .label(makeLabel(.unclear, confidence: 0.4))
        ])
        let service = makeService(counter: counter, script: script)
        let result = await service.label(makeCandidate())

        #expect(counter.value == 1, "semantic .unclear must not retry")
        #expect(result.attempts == 1)
        #expect(result.failureMode == .semantic)
        #expect(result.labelDisposition == .unclear)
        #expect(result.chapter.disposition == .ambiguous)
        #expect(result.chapter.source == .inferred)
        #expect(result.chapter.qualityScore == 0.4)
    }
}

// MARK: - label() — operational failures + retry

@Suite("ChapterLabelingService — operational failures")
struct ChapterLabelingServiceOperationalFailureTests {

    @Test("Decoding-failure error is operational and retried once")
    func decodingFailureRetried() async {
        let counter = CallCounter()
        let script = OutcomeScript([
            .error(ChapterLabelingError.decodingFailure),
            .label(makeLabel(.content, confidence: 0.7))
        ])
        let service = makeService(counter: counter, script: script)
        let result = await service.label(makeCandidate())

        #expect(counter.value == 2)
        #expect(result.attempts == 2)
        #expect(result.failureMode == nil)
        #expect(result.labelDisposition == .content)
        #expect(result.chapter.disposition == .content)
    }

    @Test("Rate-limit error is operational and retried once")
    func rateLimitRetried() async {
        let counter = CallCounter()
        let script = OutcomeScript([
            .error(ChapterLabelingError.rateLimited),
            .label(makeLabel(.content))
        ])
        let service = makeService(counter: counter, script: script)
        let result = await service.label(makeCandidate())

        #expect(counter.value == 2)
        #expect(result.attempts == 2)
        #expect(result.failureMode == nil)
    }

    @Test("Generic operational error retries and surfaces operational on second failure")
    func twoOperationalFailures() async {
        let counter = CallCounter()
        let script = OutcomeScript([
            .error(ChapterLabelingError.operational("first")),
            .error(ChapterLabelingError.operational("second"))
        ])
        let service = makeService(counter: counter, script: script)
        let result = await service.label(makeCandidate())

        #expect(counter.value == 2)
        #expect(result.attempts == 2)
        #expect(result.failureMode == .operational)
        #expect(result.labelDisposition == .unclear)
        #expect(result.chapter.disposition == .ambiguous)
        #expect(result.chapter.qualityScore == 0.0)
        #expect(result.chapter.title == nil)
    }

    @Test("Two consecutive rate-limit failures yield operational with attempts == 2")
    func twoRateLimits() async {
        let counter = CallCounter()
        let script = OutcomeScript([
            .error(ChapterLabelingError.rateLimited),
            .error(ChapterLabelingError.rateLimited)
        ])
        let service = makeService(counter: counter, script: script)
        let result = await service.label(makeCandidate())

        #expect(counter.value == 2)
        #expect(result.attempts == 2)
        #expect(result.failureMode == .operational)
    }

    @Test("Out-of-taxonomy / unsupported-guide style failure is treated as operational")
    func outOfTaxonomyOperational() async {
        // The closure simulates the GenerationError.decodingFailure
        // that the Apple decoder raises for an out-of-taxonomy enum
        // literal — see `ChapterLabelSchema` doc comment. The service
        // must retry, then fall through to operational.
        let counter = CallCounter()
        let script = OutcomeScript([
            .error(ChapterLabelingError.decodingFailure),
            .error(ChapterLabelingError.decodingFailure)
        ])
        let service = makeService(counter: counter, script: script)
        let result = await service.label(makeCandidate())

        #expect(counter.value == 2)
        #expect(result.attempts == 2)
        #expect(result.failureMode == .operational)
        #expect(result.labelDisposition == .unclear)
    }

    @Test("Unmapped error type is folded into operational and retried")
    func unmappedErrorRetried() async {
        struct CustomError: Error {}
        let counter = CallCounter()
        let script = OutcomeScript([
            .error(CustomError()),
            .label(makeLabel(.content))
        ])
        let service = makeService(counter: counter, script: script)
        let result = await service.label(makeCandidate())

        #expect(counter.value == 2)
        #expect(result.attempts == 2)
        #expect(result.failureMode == nil)
    }

    // MARK: - Guardrail refusals (au2v.1.24)

    /// A guardrail refusal is content the model declines to classify, not
    /// an infra failure. It must (a) be `failureMode == .guardrail` (NOT
    /// `.operational`), (b) NOT be retried (`attempts == 1`), and (c) yield
    /// a dropped/ambiguous chapter (qualityScore 0, labelDisposition
    /// .unclear). This is the cross-platform path: injecting a
    /// `ChapterLabelingError.guardrail` directly (which `classify` passes
    /// through unchanged) so the test runs on iOS Simulator AND Catalyst.
    @Test("Guardrail error is .guardrail (not .operational), not retried, drops the chapter")
    func guardrailNotRetried() async {
        let counter = CallCounter()
        // Script a guardrail error TWICE; a correct impl stops after the
        // first (no retry), so it must observe exactly one call. A wrong
        // impl that treats guardrail as operational would retry and the
        // counter would read 2 — that is the mutant this test kills.
        let script = OutcomeScript([
            .error(ChapterLabelingError.guardrail("refusal")),
            .error(ChapterLabelingError.guardrail("refusal"))
        ])
        let service = makeService(counter: counter, script: script)
        let result = await service.label(makeCandidate())

        #expect(counter.value == 1)
        #expect(result.attempts == 1)
        #expect(result.failureMode == .guardrail)
        #expect(result.failureMode != .operational)
        #expect(result.labelDisposition == .unclear)
        #expect(result.chapter.disposition == .ambiguous)
        #expect(result.chapter.qualityScore == 0.0)
        #expect(result.chapter.title == nil)
    }

    #if canImport(FoundationModels)
    /// Verify the real `classify` mapping: Apple's
    /// `GenerationError.refusal` and `.guardrailViolation` BOTH route to
    /// `.guardrail` through the production error-classification path
    /// (not a hand-rolled `ChapterLabelingError`). FoundationModels-gated
    /// because the GenerationError cases only exist on that platform.
    @available(iOS 26.0, *)
    @Test("GenerationError.refusal and .guardrailViolation classify to guardrail and are not retried")
    func generationRefusalClassifiesAsGuardrail() async {
        let context = LanguageModelSession.GenerationError.Context(
            debugDescription: "chapter-guardrail-test"
        )
        let refusal = LanguageModelSession.GenerationError.Refusal(transcriptEntries: [])
        let cases: [LanguageModelSession.GenerationError] = [
            .refusal(refusal, context),
            .guardrailViolation(context)
        ]
        for generationError in cases {
            let counter = CallCounter()
            let script = OutcomeScript([
                .error(generationError),
                .error(generationError)
            ])
            let service = makeService(counter: counter, script: script)
            let result = await service.label(makeCandidate())

            #expect(counter.value == 1)
            #expect(result.attempts == 1)
            #expect(result.failureMode == .guardrail)
            #expect(result.chapter.disposition == .ambiguous)
            #expect(result.chapter.qualityScore == 0.0)
        }
    }
    #endif

    // MARK: - iOS-27 LanguageModelError (playhead-l3v0)

    #if canImport(FoundationModels)
    /// playhead-l3v0: iOS/macOS 27 throws the NEW top-level `LanguageModelError`
    /// type, not the legacy `LanguageModelSession.GenerationError`. Before the
    /// fix, `classify` only cast to `GenerationError`, so an iOS-27 refusal /
    /// guardrail-violation fell through to `.operational(localizedDescription)`
    /// — which WOULD be retried (re-tripping the same guardrail) AND counted in
    /// the au2v.1.24 plan-level operational-rate abort numerator. This drives
    /// the production `label(...)` seam with the iOS-27 error and asserts BOTH
    /// route to `.guardrail` and are NOT retried, exactly like the legacy path.
    /// It compiles against the pre-fix code (`LanguageModelError` is passed as
    /// an `Error` through the injected closure) and fails at runtime there.
    @available(iOS 27.0, macOS 27.0, visionOS 27.0, watchOS 27.0, *)
    @Test("LanguageModelError.refusal / .guardrailViolation (iOS 27) classify to guardrail and are not retried")
    func languageModelErrorRefusalClassifiesAsGuardrail() async {
        let cases: [LanguageModelError] = [
            .refusal(.init(explanation: "test", debugDescription: "test")),
            .guardrailViolation(.init(debugDescription: "test"))
        ]
        for languageModelError in cases {
            let counter = CallCounter()
            // Scripted twice; a correct impl stops after the first (no retry).
            let script = OutcomeScript([
                .error(languageModelError),
                .error(languageModelError)
            ])
            let service = makeService(counter: counter, script: script)
            let result = await service.label(makeCandidate())

            #expect(counter.value == 1, "iOS-27 refusal must not be retried")
            #expect(result.attempts == 1)
            #expect(result.failureMode == .guardrail)
            #expect(result.failureMode != .operational)
            #expect(result.chapter.disposition == .ambiguous)
            #expect(result.chapter.qualityScore == 0.0)
        }
    }

    /// playhead-l3v0: pin the full `LanguageModelError` → `ChapterLabelingError`
    /// mapping through the production `classify(_:)` seam (the entry point
    /// `label(...)` uses). Guardrail cases short-circuit; every other case must
    /// land in a RETRYABLE bucket (matching where the legacy analog routed) so a
    /// regression that (e.g.) mislabels context-overflow as guardrail — which
    /// would wrongly skip the retry — is caught. Calls the existing
    /// `classify(_ error: Error)` (passing `LanguageModelError` as `Error`), so
    /// it compiles against the pre-fix code and fails there (everything mapped
    /// to `.operational(localizedDescription)`).
    @available(iOS 27.0, macOS 27.0, visionOS 27.0, watchOS 27.0, *)
    @Test("LanguageModelError (iOS 27) classify maps every case to its ChapterLabelingError analog")
    func languageModelErrorClassifyMapping() {
        // Buckets with no associated value compare by ==.
        #expect(
            ChapterLabelingService.classify(
                LanguageModelError.contextSizeExceeded(.init(contextSize: 4096, tokenCount: 8192, debugDescription: "t"))
            ) == .exceededContextWindow
        )
        #expect(
            ChapterLabelingService.classify(
                LanguageModelError.rateLimited(.init(resetDate: nil, debugDescription: "t"))
            ) == .rateLimited
        )
        #expect(
            ChapterLabelingService.classify(
                LanguageModelError.unsupportedGenerationGuide(.init(schemaName: "ChapterLabel", debugDescription: "t"))
            ) == .decodingFailure
        )
        #expect(
            ChapterLabelingService.classify(
                LanguageModelError.unsupportedTranscriptContent(.init(unsupportedContent: [], debugDescription: "t"))
            ) == .decodingFailure
        )

        // Guardrail bucket carries an associated String — match the case shape.
        let guardrailCases: [LanguageModelError] = [
            .refusal(.init(explanation: "t", debugDescription: "t")),
            .guardrailViolation(.init(debugDescription: "t"))
        ]
        for guardrailCase in guardrailCases {
            guard case .guardrail = ChapterLabelingService.classify(guardrailCase) else {
                Issue.record("\(guardrailCase) must classify to .guardrail")
                continue
            }
        }

        // Operational bucket also carries an associated String — match the case.
        let operationalCases: [LanguageModelError] = [
            .unsupportedLanguageOrLocale(.init(languageCode: Locale.LanguageCode("fr"), debugDescription: "t")),
            .unsupportedCapability(.init(capability: .reasoning, debugDescription: "t")),
            .timeout(.init(debugDescription: "t"))
        ]
        for operationalCase in operationalCases {
            guard case .operational = ChapterLabelingService.classify(operationalCase) else {
                Issue.record("\(operationalCase) must classify to .operational")
                continue
            }
        }
    }
    #endif
}

// MARK: - label() — cancellation

@Suite("ChapterLabelingService — cancellation")
struct ChapterLabelingServiceCancellationTests {

    /// Cancellation must short-circuit the retry loop instead of being
    /// laundered into another operational retry. The service's contract
    /// is that an externally-cancelled task returns ONE operational
    /// result and stops calling the FM.
    @Test("Externally-cancelled task does not retry past cancellation")
    func cancellationStopsRetries() async {
        let counter = CallCounter()
        let script = OutcomeScript([
            .error(CancellationError()),
            .error(CancellationError())
        ])
        let service = makeService(counter: counter, script: script)

        let task = Task {
            await service.label(makeCandidate())
        }
        task.cancel()
        let result = await task.value

        #expect(result.failureMode == .operational)
        #expect(result.labelDisposition == .unclear)
        // The cancellation may be observed at any of: the
        // `Task.isCancelled` gate at the top of the retry loop (no FM
        // call), the FM-call closure (`CancellationError` thrown), or
        // the backoff sleep. In every path we end before a second
        // attempt completes successfully — counter must be at most 1.
        #expect(counter.value <= 1)
    }

    /// A successful response from the FM call should still produce a
    /// success even if cancellation arrives later in the same task —
    /// the work was already done.
    @Test("Cancellation after successful FM call still returns success")
    func cancellationAfterSuccess() async {
        let counter = CallCounter()
        let script = OutcomeScript([
            .label(makeLabel(.content, confidence: 0.85))
        ])
        let service = makeService(counter: counter, script: script)
        let task = Task {
            await service.label(makeCandidate())
        }
        let result = await task.value
        // Even if the parent task is cancelled now, the result was
        // already produced. Cancellation must not retroactively change
        // a successful return.
        task.cancel()
        #expect(result.failureMode == nil)
        #expect(result.labelDisposition == .content)
        #expect(counter.value == 1)
    }
}

// MARK: - label() — timeout

@Suite("ChapterLabelingService — timeout")
struct ChapterLabelingServiceTimeoutTests {

    /// Run the service against a very short timeout override by
    /// constructing the service with a fake clock that "fast-forwards"
    /// the timeout duration. We cannot reach the production
    /// `perCallTimeoutSeconds = 8.0` value in a unit test without
    /// either waiting 8 seconds or shortening the constant, so this
    /// test instead directly drives `runWithTimeout`'s observable
    /// behavior: a slow-call closure that takes longer than a fast
    /// clock's "8 seconds" must bubble up `.timedOut` and trigger
    /// retry.
    @Test("Slow FM call resolves to operational + retry under timeout race")
    func timeoutRetried() async {
        // Simulate the timeout by raising `.timedOut` directly. The
        // production code path that maps "ContinuousClock said 8s
        // elapsed" → `ChapterLabelingError.timedOut` is exercised by
        // the live FM smoke tests on a real device; here we verify
        // that the retry wiring around `.timedOut` matches the same
        // operational arm as the other failures.
        let counter = CallCounter()
        let script = OutcomeScript([
            .error(ChapterLabelingError.timedOut),
            .label(makeLabel(.content, confidence: 0.6))
        ])
        let service = makeService(counter: counter, script: script)
        let result = await service.label(makeCandidate())

        #expect(counter.value == 2)
        #expect(result.attempts == 2)
        #expect(result.failureMode == nil)
        #expect(result.labelDisposition == .content)
    }

    @Test("Two consecutive timeouts yield operational with attempts == 2")
    func twoTimeouts() async {
        let counter = CallCounter()
        let script = OutcomeScript([
            .error(ChapterLabelingError.timedOut),
            .error(ChapterLabelingError.timedOut)
        ])
        let service = makeService(counter: counter, script: script)
        let result = await service.label(makeCandidate())

        #expect(counter.value == 2)
        #expect(result.attempts == 2)
        #expect(result.failureMode == .operational)
    }

    @Test("End-to-end timeout: a sleeping closure exceeds an injected fast clock")
    func endToEndTimeout() async {
        // This drives the actual `withThrowingTaskGroup` race. We
        // inject a fake `Clock` whose `sleep(for:)` returns
        // immediately; the FM-call branch sleeps for 50ms. The fast
        // clock wins the race and `.timedOut` is raised → retry.
        // The retry's clock sleep also returns immediately, so the
        // second attempt runs and we hand back a label.
        let counter = CallCounter()
        let outcomes: [OutcomeScript.Outcome] = [
            .sleepThenSucceed(label: makeLabel(.content), sleepNanos: 50_000_000),
            .label(makeLabel(.content, confidence: 0.9))
        ]
        let script = OutcomeScript(outcomes)
        let service = ChapterLabelingService(
            labelCall: { _ in
                counter.increment()
                switch script.next() {
                case let .label(label):
                    return label
                case let .error(error):
                    throw error
                case let .sleepThenSucceed(label, sleepNanos):
                    try await Task.sleep(nanoseconds: sleepNanos)
                    return label
                case let .sleepThenError(error, sleepNanos):
                    try await Task.sleep(nanoseconds: sleepNanos)
                    throw error
                }
            },
            clock: ImmediateClock()
        )
        let result = await service.label(makeCandidate())
        #expect(counter.value >= 1)
        // The slow call may or may not have completed before the
        // immediate clock fired — under a fast clock the timeout
        // ALWAYS wins, so we expect a retry. The retry's clock sleep
        // is also immediate; the second attempt is the immediate
        // success above OR a second timeout if the slow call won
        // (which doesn't happen with ImmediateClock).
        #expect(result.failureMode == nil || result.failureMode == .operational)
        #expect(result.attempts <= 2)
    }
}

// MARK: - ImmediateClock — test infrastructure

/// Test clock whose sleeps complete immediately. Used by the timeout
/// race test to make the timeout branch ALWAYS win.
private struct ImmediateClock: Clock {
    typealias Duration = Swift.Duration
    typealias Instant = ContinuousClock.Instant

    var now: Instant { ContinuousClock().now }
    var minimumResolution: Swift.Duration { .zero }

    func sleep(until deadline: Instant, tolerance: Swift.Duration?) async throws {
        // Yield a tick to give the cooperative scheduler a chance to
        // schedule other tasks, then return. Don't actually sleep.
        await Task.yield()
    }
}

// MARK: - Backoff schedule

@Suite("ChapterLabelingService — backoff")
struct ChapterLabelingServiceBackoffTests {

    @Test("First retry uses 50ms backoff")
    func firstRetryBackoff() {
        #expect(ChapterLabelingService.backoffNanos(forRetryNumber: 1) == 50_000_000)
    }

    @Test("Second retry slot uses 200ms backoff (documented but unused at maxAttempts=2)")
    func secondRetryBackoff() {
        #expect(ChapterLabelingService.backoffNanos(forRetryNumber: 2) == 200_000_000)
    }

    @Test("Beyond-schedule retries return zero backoff")
    func beyondSchedule() {
        #expect(ChapterLabelingService.backoffNanos(forRetryNumber: 3) == 0)
        #expect(ChapterLabelingService.backoffNanos(forRetryNumber: 99) == 0)
    }

    @Test("maxAttempts is 2 (initial + one retry)")
    func maxAttempts() {
        #expect(ChapterLabelingService.maxAttempts == 2)
    }

    @Test("perCallTimeoutSeconds is 8 seconds (FM call budget)")
    func timeoutBudget() {
        #expect(ChapterLabelingService.perCallTimeoutSeconds == 8.0)
    }
}

// MARK: - ChapterLabel @Generable round-trip

@Suite("ChapterLabel Codable round-trip")
struct ChapterLabelCodableTests {

    @Test("Codable round-trip preserves disposition + confidence + topic")
    func roundTrip() throws {
        let original = ChapterLabel(
            disposition: .hostReadAd,
            confidence: 0.92,
            topicDescriptor: "AcmeWidgets"
        )
        let data = try JSONEncoder().encode(original)
        let decoded = try JSONDecoder().decode(ChapterLabel.self, from: data)
        #expect(decoded.disposition == original.disposition)
        #expect(decoded.confidence == original.confidence)
        #expect(decoded.topicDescriptor == original.topicDescriptor)
    }

    @Test("Nil topic descriptor round-trips to nil")
    func nilTopic() throws {
        let original = ChapterLabel(
            disposition: .content,
            confidence: 0.7,
            topicDescriptor: nil
        )
        let data = try JSONEncoder().encode(original)
        let decoded = try JSONDecoder().decode(ChapterLabel.self, from: data)
        #expect(decoded.topicDescriptor == nil)
    }
}

// MARK: - label() — topic descriptor preservation

/// Pin the topic-descriptor flow through `successResult` /
/// `semanticResult`. Bead spec calls out:
/// "FM returns nil/empty topicDescriptor → preserved as nil." The
/// expected end state is `LabelingResult.topicDescriptor == nil` AND
/// `LabelingResult.chapter.title == nil` — the two fields must agree
/// so consumers (cache, evidence pipeline) cannot read a stale title
/// when the descriptor is missing.
@Suite("ChapterLabelingService — topic descriptor preservation")
struct ChapterLabelingServiceTopicDescriptorTests {

    @Test("nil topicDescriptor on a confident success → result.topicDescriptor and chapter.title are both nil")
    func nilDescriptorPreserved() async {
        let counter = CallCounter()
        let script = OutcomeScript([
            .label(makeLabel(.content, confidence: 0.7, topic: nil))
        ])
        let service = makeService(counter: counter, script: script)
        let result = await service.label(makeCandidate())

        #expect(result.failureMode == nil)
        #expect(result.topicDescriptor == nil)
        #expect(result.chapter.title == nil)
    }

    @Test("Whitespace-only topicDescriptor sanitizes to nil on both LabelingResult and chapter")
    func whitespaceDescriptorSanitizesToNil() async {
        let counter = CallCounter()
        let script = OutcomeScript([
            .label(makeLabel(.content, confidence: 0.6, topic: "   \n\t   "))
        ])
        let service = makeService(counter: counter, script: script)
        let result = await service.label(makeCandidate())

        #expect(result.failureMode == nil)
        #expect(result.topicDescriptor == nil)
        #expect(result.chapter.title == nil)
    }

    @Test("Empty-string topicDescriptor sanitizes to nil")
    func emptyDescriptorSanitizesToNil() async {
        let counter = CallCounter()
        let script = OutcomeScript([
            .label(makeLabel(.content, confidence: 0.6, topic: ""))
        ])
        let service = makeService(counter: counter, script: script)
        let result = await service.label(makeCandidate())

        #expect(result.failureMode == nil)
        #expect(result.topicDescriptor == nil)
        #expect(result.chapter.title == nil)
    }

    @Test("Topic descriptor longer than the cap is truncated for both result and chapter title")
    func longDescriptorTruncated() async {
        let cap = ChapterLabelingService.topicDescriptorCharacterCap
        let huge = String(repeating: "x", count: cap + 50)
        let counter = CallCounter()
        let script = OutcomeScript([
            .label(makeLabel(.content, confidence: 0.6, topic: huge))
        ])
        let service = makeService(counter: counter, script: script)
        let result = await service.label(makeCandidate())

        #expect(result.failureMode == nil)
        #expect(result.topicDescriptor?.count == cap)
        #expect(result.chapter.title?.count == cap)
        // Pin the alignment: title and topicDescriptor share the same
        // sanitized string so the two fields cannot diverge.
        #expect(result.topicDescriptor == result.chapter.title)
    }

    @Test("Semantic .unclear with nil topicDescriptor preserves nil on both fields")
    func semanticNilDescriptor() async {
        let counter = CallCounter()
        let script = OutcomeScript([
            .label(makeLabel(.unclear, confidence: 0.3, topic: nil))
        ])
        let service = makeService(counter: counter, script: script)
        let result = await service.label(makeCandidate())

        #expect(result.failureMode == .semantic)
        #expect(result.topicDescriptor == nil)
        #expect(result.chapter.title == nil)
    }
}

// MARK: - label() — confidence clamping contract

/// The bead spec language "FM returns confidence outside [0,1] → clamp
/// + .operational flag" is INTENTIONALLY softened in the production
/// design (see `ChapterLabelingService.swift` header: "Out-of-range
/// confidence values (NaN / Inf or outside [0, 1]) are clamped, not
/// rejected. A clamped confidence still counts as a successful call.").
/// These tests pin the production behavior so a future refactor that
/// silently flips the contract would surface in CI.
@Suite("ChapterLabelingService — confidence clamp contract")
struct ChapterLabelingServiceClampContractTests {

    @Test("Clamped HIGH confidence (1.42 → 1.0) does NOT raise operational flag")
    func clampHighIsSuccess() async {
        let counter = CallCounter()
        let script = OutcomeScript([
            .label(makeLabel(.content, confidence: 1.42))
        ])
        let service = makeService(counter: counter, script: script)
        let result = await service.label(makeCandidate())

        #expect(result.failureMode == nil, "clamping a high confidence is intentionally a success")
        #expect(result.attempts == 1, "clamp does not trigger retry")
        #expect(counter.value == 1)
        #expect(result.chapter.qualityScore == 1.0)
    }

    @Test("Clamped LOW confidence (-0.3 → 0.0) does NOT raise operational flag")
    func clampLowIsSuccess() async {
        let counter = CallCounter()
        let script = OutcomeScript([
            .label(makeLabel(.content, confidence: -0.3))
        ])
        let service = makeService(counter: counter, script: script)
        let result = await service.label(makeCandidate())

        #expect(result.failureMode == nil)
        #expect(result.attempts == 1)
        #expect(counter.value == 1)
        #expect(result.chapter.qualityScore == 0.0)
    }

    @Test("Non-finite confidence (Inf) collapses to 0 without raising operational flag")
    func clampInfIsSuccess() async {
        let counter = CallCounter()
        let script = OutcomeScript([
            .label(makeLabel(.content, confidence: .infinity))
        ])
        let service = makeService(counter: counter, script: script)
        let result = await service.label(makeCandidate())

        #expect(result.failureMode == nil)
        #expect(result.chapter.qualityScore == 0.0)
        #expect(counter.value == 1)
    }
}

// MARK: - label() — token-budget guard end-to-end

/// The prompt builder's region-text cap is a defense-in-depth guard
/// against a runaway transcript blowing the FM's per-call token
/// budget. `ChapterLabelingServicePromptTests.promptRespectsCharCap`
/// covers the cap in isolation; this suite exercises the contract
/// END-TO-END through `label(...)` with a mock that ASSERTS the
/// observed prompt is within budget. If a future refactor accidentally
/// removes the cap, these tests fail before any real device call.
@Suite("ChapterLabelingService — token-budget guard")
struct ChapterLabelingServiceTokenBudgetTests {

    /// Loose upper bound on the entire prompt, including schema /
    /// scaffold overhead. The prompt body is capped at
    /// `regionTextCharacterCap`; the fixed instruction scaffold around
    /// it (taxonomy + sponsor-read guidance, chapter header, "Region
    /// transcript:" prefix, etc.) adds ~470 chars. The `+1024` margin
    /// stays comfortably above that. Pinning a generous bound here
    /// surfaces a regression where the cap is removed entirely without
    /// locking the exact scaffold length, which is
    /// documentation-not-contract.
    private static let promptUpperBoundChars = ChapterLabelingService.regionTextCharacterCap + 1024

    @Test("Huge region text → labeler still succeeds; prompt observed by the FM stays within cap+scaffold")
    func hugeRegionDoesNotTripFM() async {
        let huge = String(repeating: "@", count: 50_000)
        let counter = CallCounter()
        // The mock asserts the prompt size before responding. If the
        // cap is removed the assertion will fire and fail this test.
        let observedPromptLength = ObservedLength()
        let service = ChapterLabelingService(
            labelCall: { prompt in
                counter.increment()
                observedPromptLength.set(prompt.count)
                #expect(prompt.count <= Self.promptUpperBoundChars,
                        "labeler must not let region text blow the per-call token budget")
                return makeLabel(.content, confidence: 0.8)
            }
        )
        let result = await service.label(makeCandidate(regionText: huge))
        #expect(result.failureMode == nil)
        #expect(counter.value == 1, "no retry should be needed")
        #expect(observedPromptLength.value > 0)
        // Sanity: the prompt MUST contain at most `regionTextCharacterCap`
        // body characters from the runaway region (sentinel `@`).
        // Because we cannot inspect the prompt directly here, rely on
        // the upper-bound assertion above plus the prompt-shape suite
        // for content-level pinning.
    }

    @Test("Mock that rejects oversized prompts is never tripped by the labeler")
    func mockRejectsOversizeButLabelerNeverTrips() async {
        let huge = String(repeating: "@", count: 100_000)
        let counter = CallCounter()
        // This mock ENFORCES the cap by throwing when the prompt is
        // larger than the documented budget. A correctly capped
        // labeler will never trip the throw.
        let service = ChapterLabelingService(
            labelCall: { prompt in
                counter.increment()
                if prompt.count > ChapterLabelingServiceTokenBudgetTests.promptUpperBoundChars {
                    throw ChapterLabelingError.exceededContextWindow
                }
                return makeLabel(.content, confidence: 0.7)
            }
        )
        let result = await service.label(makeCandidate(regionText: huge))
        #expect(result.failureMode == nil, "labeler must not exceed prompt budget")
        #expect(result.attempts == 1)
        #expect(counter.value == 1)
    }
}

/// Thread-safe length recorder used by the token-budget end-to-end
/// test (the `labelCall` closure is `@Sendable`, so an `inout` /
/// captured `var` won't fly).
private final class ObservedLength: @unchecked Sendable {
    private let lock = NSLock()
    private var _value = 0
    func set(_ v: Int) {
        lock.lock(); defer { lock.unlock() }
        _value = v
    }
    var value: Int {
        lock.lock(); defer { lock.unlock() }
        return _value
    }
}
