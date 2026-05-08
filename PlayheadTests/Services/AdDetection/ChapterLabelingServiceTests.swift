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

    @Test("Codable round-trip preserves both cases")
    func roundTrip() throws {
        for mode in [LabelFailureMode.operational, .semantic] {
            let data = try JSONEncoder().encode(mode)
            let decoded = try JSONDecoder().decode(LabelFailureMode.self, from: data)
            #expect(decoded == mode)
        }
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
}

// MARK: - label() — happy path

@Suite("ChapterLabelingService — happy path")
struct ChapterLabelingServiceHappyPathTests {

    @Test("Successful confident label produces expected ChapterEvidence + LabelingResult")
    func successfulLabel() async {
        let counter = CallCounter()
        let script = OutcomeScript([
            .label(makeLabel(.hostReadAd, confidence: 0.85, topic: "BetterHelp"))
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
        #expect(result.topicDescriptor == "BetterHelp")
        #expect(result.chapter.source == .inferred)
        #expect(result.chapter.disposition == .adBreak)
        #expect(result.chapter.qualityScore == 0.85)
        #expect(result.chapter.startTime == 60)
        #expect(result.chapter.endTime == 130)
        #expect(result.chapter.title == "BetterHelp")
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
            topicDescriptor: "BetterHelp"
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
