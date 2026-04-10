// KellyRipaFMSafetyTests.swift
// Unit tests for the three Kelly Ripa FM safety improvements:
//
//   playhead-994: includeSchemaInPrompt: false experiment in refinePassB
//   playhead-36t: capture refusal.explanation in FM classifier catch blocks
//   playhead-eu1: auto-retry default-path GenerationError.refusal via permissive path
//
// All tests run on the simulator using the TestFMRuntime / RuntimeRecorder
// stubs — no real Foundation Models stack required.

import Foundation
import Testing

@testable import Playhead

#if canImport(FoundationModels)
import FoundationModels
#endif

// MARK: - playhead-36t: refusal explanation captured in diagnostic

@Suite("playhead-36t — refusal explanation captured in diagnostic observer")
struct PlayheadRefusalExplanationTests {

    // MARK: - Fixtures

    private func makeRefinementPlan(windowIndex: Int = 0) -> RefinementWindowPlan {
        RefinementWindowPlan(
            windowIndex: windowIndex,
            sourceWindowIndex: windowIndex,
            lineRefs: [4, 5],
            focusLineRefs: [4, 5],
            focusClusters: [[4, 5]],
            prompt: "Refine ad spans.\nL4> \"Hey everyone, it's Kelly Ripa\"\nL5> \"Let's talk off camera\"",
            promptTokenCount: 12,
            startTime: 40.0,
            endTime: 50.0,
            stopReason: .minimumSpan,
            promptEvidence: []
        )
    }

    private func makeSegments(for plan: RefinementWindowPlan) -> [AdTranscriptSegment] {
        plan.lineRefs.map { lineRef in
            let start = Double(lineRef) * 5
            let end = start + 5
            let lineSpec: (start: Double, end: Double, text: String) = (start: start, end: end, text: "line \(lineRef)")
            return makeFMSegments(
                analysisAssetId: "asset-36t",
                transcriptVersion: "tx-36t",
                lines: [lineSpec]
            ).first!
        }
    }

    // MARK: - 36t test 1: refusalExplanation field defaults to nil on successful scan

    @Test("playhead-36t: SemanticScanResult.refusalExplanation defaults to nil for successful row")
    func refusalExplanationNilOnSuccess() {
        let result = SemanticScanResult(
            id: "sr-36t-nil",
            analysisAssetId: "asset-36t",
            windowFirstAtomOrdinal: 0,
            windowLastAtomOrdinal: 0,
            windowStartTime: 0,
            windowEndTime: 10,
            scanPass: "passB",
            transcriptQuality: .good,
            disposition: .noAds,
            spansJSON: "[]",
            status: .success,
            attemptCount: 1,
            errorContext: nil,
            inputTokenCount: nil,
            outputTokenCount: nil,
            latencyMs: nil,
            prewarmHit: false,
            scanCohortJSON: "{}",
            transcriptVersion: "tx-36t"
        )
        #expect(result.refusalExplanation == nil)
    }

    // MARK: - 36t test 2: refusalExplanation is propagated when set

    @Test("playhead-36t: SemanticScanResult.refusalExplanation is persisted when set on init")
    func refusalExplanationPersistedWhenSet() {
        let explanation = "The classifier flagged schema-injected ad-classification tokens"
        let result = SemanticScanResult(
            id: "sr-36t-set",
            analysisAssetId: "asset-36t",
            windowFirstAtomOrdinal: 0,
            windowLastAtomOrdinal: 0,
            windowStartTime: 0,
            windowEndTime: 10,
            scanPass: "passB",
            transcriptQuality: .good,
            disposition: .noAds,
            spansJSON: "[]",
            status: .refusal,
            attemptCount: 1,
            errorContext: nil,
            inputTokenCount: nil,
            outputTokenCount: nil,
            latencyMs: nil,
            prewarmHit: false,
            scanCohortJSON: "{}",
            transcriptVersion: "tx-36t",
            refusalExplanation: explanation
        )
        #expect(result.refusalExplanation == explanation)
    }

    // MARK: - 36t test 3: diagnostic observer receives refusalExplanation field

    @Test("playhead-36t: refinement refusal diagnostic has refusalExplanation field (nil when no FM backend)")
    func refinementRefusalDiagnosticHasExplanationField() async throws {
        // The diagnostic now includes a refusalExplanation field. On the
        // simulator (no real Foundation Models), explanation is always nil
        // because GenerationError.Refusal.explanation makes a live FM call.
        // This test verifies the diagnostic is emitted AND the field exists
        // (even if nil in the absence of a real model).
        let captureBox = DiagnosticCaptureBox()
        FoundationModelClassifier.refinementRefusalDiagnosticObserver = { diag in
            captureBox.append(diag)
        }
        defer { FoundationModelClassifier.refinementRefusalDiagnosticObserver = nil }

        let plan = makeRefinementPlan(windowIndex: 0)
        let runtime = FakeRefusalRuntime(
            planCount: 1,
            failureOnWindow: 0
        )
        let classifier = FoundationModelClassifier(runtime: runtime.runtime)
        let segments = [
            makeFMSegments(
                analysisAssetId: "asset-36t-diag",
                transcriptVersion: "tx-36t-diag",
                lines: [
                    (0, 5, "Hey everyone, it's Kelly Ripa"),
                    (5, 10, "Let's talk off camera")
                ]
            )
        ].flatMap { $0 }
        let catalog = EvidenceCatalog(
            analysisAssetId: "asset-36t-diag",
            transcriptVersion: "tx-36t-diag",
            entries: []
        )

        _ = try await classifier.refinePassB(
            zoomPlans: [plan],
            segments: segments,
            evidenceCatalog: catalog
        )

        let captured = captureBox.snapshot()
        // The refusal path fires at least once for the single window.
        // (May fire more than once if the runtime triggers a retry attempt,
        // depending on the status mapped from the simulated error.)
        #expect(
            !captured.isEmpty,
            "expected at least 1 refusal diagnostic, got 0"
        )
        // The refusalExplanation field must exist on the diagnostic type
        // (may be nil on the simulator since no real FM call is made).
        let diag = try #require(captured.first)
        #expect(diag.windowIndex == 0)
        // refusalExplanation is nil in test (no real FM backend).
        // The field itself must be present in the struct (compile-time proof).
        let _ = diag.refusalExplanation
    }
}

// MARK: - playhead-eu1: auto-retry refusals via permissive path

@Suite("playhead-eu1 — auto-retry default-path refusals via permissive path")
struct PlayheadEu1AutoRetryTests {

    // MARK: - eu1 test 1: refusal triggers permissive retry (on-device only)

    // NOTE: This path cannot be unit-tested on the simulator.
    //
    // The conditional at FoundationModelClassifier.swift line ~1799 fires when
    // `status == .refusal` and calls `permissive.refine(...)`. To exercise that
    // branch in a simulator test we need `permissive.refine(...)` to succeed and
    // return a `.spans([...])` result. However:
    //
    //   - `PermissiveAdClassifier` is a concrete actor, not a protocol, so there
    //     is no injection seam for a hand-rolled mock that returns `.spans`
    //     without real Foundation Models.
    //   - The `#if DEBUG` fault-injection hook on `PermissiveAdClassifier` only
    //     supports throwing a `PermissiveClassificationError` (or arbitrary
    //     `Error`); there is no hook to return a successful `.spans` value.
    //   - In non-FoundationModels simulator builds, `PermissiveAdClassifier.refine`
    //     falls through to its `#else` branch and always returns `.noAd`, so
    //     `permissiveSpans` is always empty and the `windows.append(...)` branch
    //     never fires.
    //
    // The correct coverage vehicle is an on-device smoke test:
    //   1. Construct `FoundationModelClassifier` with a live FM runtime.
    //   2. Feed a window whose transcript text reliably triggers `@Generable`
    //      refusal (e.g. a Kelly Ripa cross-promo line with pharma vocabulary).
    //   3. Inject a `PermissiveAdClassifier` with no fault installed.
    //   4. Call `refinePassB(zoomPlans:segments:evidenceCatalog:
    //      sensitiveRouter:permissiveClassifier:)`.
    //   5. Assert `output.windows.first?.usedPermissiveFallback == true` and
    //      `output.windows.first?.spans.isEmpty == false`.
    //
    // See `PlayheadFMSmokeTests` for the on-device harness that already validates
    // the permissive classifier's response quality.
    //
    // eu1 test 2 (`refusalWithoutPermissiveClassifierRemainsFailure`) covers the
    // nil-classifier branch of the same conditional on the simulator via
    // `FakeRefusalRuntime`, which IS feasible without real FM.
    @available(iOS 26.0, *)
    @Test(
        "playhead-eu1: @Generable refusal + permissive retry (on-device only — see comment)",
        .disabled("Requires live Foundation Models; run on a real device via PlayheadFMSmokeTests")
    )
    func genreableRefusalTriggersPermissiveRetry() async throws {
        // This test is intentionally disabled on the simulator. The path it
        // exercises (refinePassB → status == .refusal → permissive.refine →
        // windows.append(usedPermissiveFallback: true)) requires both a real
        // Foundation Models refusal AND a real PermissiveAdClassifier that
        // can respond with spans. Neither is available without live FM.
        //
        // If you are running this on a real device, remove the `.disabled`
        // trait above and re-enable.
        Issue.record("on-device only test ran in simulator context")
    }

    // MARK: - eu1 test 2: no permissive classifier → refusal stays as failed window

    @Test("playhead-eu1: refusal with nil permissive classifier remains a failed window (no retry)")
    func refusalWithoutPermissiveClassifierRemainsFailure() async throws {
        // Drive refinePassB with one window that refuses, and NO permissive
        // classifier. The window must appear in failedWindowStatuses as .refusal.
        let plan = makeSingleRefusalPlan(windowIndex: 0)
        let runtime = FakeRefusalRuntime(planCount: 1, failureOnWindow: 0)
        let classifier = FoundationModelClassifier(runtime: runtime.runtime)
        let segments = makeKellyRipaSegments()
        let catalog = makeEmptyCatalog(for: segments)

        let output = try await classifier.refinePassB(
            zoomPlans: [plan],
            segments: segments,
            evidenceCatalog: catalog
        )

        #expect(output.failedWindowStatuses == [.refusal])
        #expect(output.windows.isEmpty, "refused window with no permissive fallback must not produce output")
    }

    // MARK: - eu1 test 3: success path does not invoke permissive (no double-call)

    @available(iOS 26.0, *)
    @Test("playhead-eu1: @Generable success does not trigger permissive retry")
    func genereableSuccessDoesNotTriggerPermissiveRetry() async throws {
        // Drive refinePassB with one window that SUCCEEDS. Install a permissive
        // classifier whose fault-injection hook records whether it was called.
        let permissive = PermissiveAdClassifier()
        let callCounter = CallCounterBox()
        await permissive.installFaultInjectionForTesting { _ in
            // This closure is called IFF the permissive path fires.
            callCounter.increment()
            // Return nil so the call would succeed if it fires.
            return nil
        }

        let plan = makeRefinementPlan(windowIndex: 0)
        let runtime = FakeSuccessRuntime(planCount: 1)
        let classifier = FoundationModelClassifier(runtime: runtime.runtime)
        let segments = makeKellyRipaSegments()
        let catalog = makeEmptyCatalog(for: segments)

        let router = SensitiveWindowRouter(triggerRules: [])  // noop router

        _ = try await classifier.refinePassB(
            zoomPlans: [plan],
            segments: segments,
            evidenceCatalog: catalog,
            sensitiveRouter: router,
            permissiveClassifier: permissive
        )

        #expect(
            callCounter.value == 0,
            "permissive classifier must NOT be called when @Generable succeeds; call count=\(callCounter.value)"
        )
    }

    // MARK: - eu1 test 4: usedPermissiveFallback is false on normal refinement window

    @Test("playhead-eu1: FMRefinementWindowOutput.usedPermissiveFallback defaults to false")
    func fallbackFlagDefaultsFalse() {
        let output = FMRefinementWindowOutput(
            windowIndex: 1,
            sourceWindowIndex: 1,
            lineRefs: [1, 2],
            spans: [],
            latencyMillis: 5
        )
        #expect(output.usedPermissiveFallback == false)
        #expect(output.permissiveFallbackReason == nil)
    }

    // MARK: - eu1 test 5: SemanticScanResult eu1 fields propagated

    @Test("playhead-eu1: SemanticScanResult.usedPermissiveFallback defaults to false")
    func semanticScanResultFallbackDefault() {
        let result = SemanticScanResult(
            id: "sr-eu1-default",
            analysisAssetId: "asset-eu1",
            windowFirstAtomOrdinal: 0,
            windowLastAtomOrdinal: 0,
            windowStartTime: 0,
            windowEndTime: 10,
            scanPass: "passB",
            transcriptQuality: .good,
            disposition: .containsAd,
            spansJSON: "[]",
            status: .success,
            attemptCount: 1,
            errorContext: nil,
            inputTokenCount: nil,
            outputTokenCount: nil,
            latencyMs: nil,
            prewarmHit: false,
            scanCohortJSON: "{}",
            transcriptVersion: "tx-eu1"
        )
        #expect(result.usedPermissiveFallback == false)
        #expect(result.permissiveFallbackReason == nil)
    }

    @Test("playhead-eu1: SemanticScanResult.usedPermissiveFallback and reason are set when fallback used")
    func semanticScanResultFallbackSet() {
        let reason = "May contain sensitive content"
        let result = SemanticScanResult(
            id: "sr-eu1-set",
            analysisAssetId: "asset-eu1",
            windowFirstAtomOrdinal: 0,
            windowLastAtomOrdinal: 0,
            windowStartTime: 0,
            windowEndTime: 10,
            scanPass: "passB",
            transcriptQuality: .good,
            disposition: .containsAd,
            spansJSON: "[]",
            status: .success,
            attemptCount: 1,
            errorContext: nil,
            inputTokenCount: nil,
            outputTokenCount: nil,
            latencyMs: nil,
            prewarmHit: false,
            scanCohortJSON: "{}",
            transcriptVersion: "tx-eu1",
            usedPermissiveFallback: true,
            permissiveFallbackReason: reason
        )
        #expect(result.usedPermissiveFallback == true)
        #expect(result.permissiveFallbackReason == reason)
    }
}

// MARK: - Helpers

private func makeRefinementPlan(windowIndex: Int = 0) -> RefinementWindowPlan {
    RefinementWindowPlan(
        windowIndex: windowIndex,
        sourceWindowIndex: windowIndex,
        lineRefs: [4, 5],
        focusLineRefs: [4, 5],
        focusClusters: [[4, 5]],
        prompt: "Refine ad spans.\nL4> \"Hey everyone, it's Kelly Ripa\"\nL5> \"Let's talk off camera\"",
        promptTokenCount: 12,
        startTime: 40.0,
        endTime: 50.0,
        stopReason: .minimumSpan,
        promptEvidence: []
    )
}

private func makeSingleRefusalPlan(windowIndex: Int = 0) -> RefinementWindowPlan {
    makeRefinementPlan(windowIndex: windowIndex)
}

private func makeKellyRipaSegments() -> [AdTranscriptSegment] {
    makeFMSegments(
        analysisAssetId: "asset-eu1-kr",
        transcriptVersion: "tx-eu1-kr",
        lines: [
            (0, 5, "Hey everyone, it's Kelly Ripa"),
            (5, 10, "Let's talk off camera")
        ]
    )
}

private func makeEmptyCatalog(for segments: [AdTranscriptSegment]) -> EvidenceCatalog {
    EvidenceCatalog(
        analysisAssetId: segments.first?.firstAtomOrdinal.description ?? "asset-eu1",
        transcriptVersion: "tx-eu1",
        entries: []
    )
}

private func makeEmptyCatalog(assetId: String = "asset-eu1", txVersion: String = "tx-eu1") -> EvidenceCatalog {
    EvidenceCatalog(
        analysisAssetId: assetId,
        transcriptVersion: txVersion,
        entries: []
    )
}

// MARK: - Thread-safe call counter

private final class CallCounterBox: @unchecked Sendable {
    private let lock = NSLock()
    private var _value = 0
    func increment() { lock.lock(); defer { lock.unlock() }; _value += 1 }
    var value: Int { lock.lock(); defer { lock.unlock() }; return _value }
}

// MARK: - Diagnostic capture box

private final class DiagnosticCaptureBox: @unchecked Sendable {
    private let lock = NSLock()
    private var items: [FoundationModelClassifier.RefinementPassRefusalDiagnostic] = []
    func append(_ d: FoundationModelClassifier.RefinementPassRefusalDiagnostic) {
        lock.lock(); defer { lock.unlock() }
        items.append(d)
    }
    func snapshot() -> [FoundationModelClassifier.RefinementPassRefusalDiagnostic] {
        lock.lock(); defer { lock.unlock() }
        return items
    }
}

// MARK: - Fake runtime that always refuses one specific window

private actor FakeRefusalRuntime {
    private let planCount: Int
    nonisolated let failureOnWindow: Int
    private var callIndex = 0

    init(planCount: Int, failureOnWindow: Int) {
        self.planCount = planCount
        self.failureOnWindow = failureOnWindow
    }

    nonisolated var runtime: FoundationModelClassifier.Runtime {
        FoundationModelClassifier.Runtime(
            availabilityStatus: { _ in nil },
            contextSize: { 4096 },
            tokenCount: { _ in 4 },
            coarseSchemaTokenCount: { 8 },
            refinementSchemaTokenCount: { 16 },
            makeSession: {
                let callIndex = await self.nextCallIndex()
                return FoundationModelClassifier.Runtime.Session(
                    prewarm: { _ in },
                    respondCoarse: { _ in
                        CoarseScreeningSchema(disposition: .containsAd, support: nil)
                    },
                    respondRefinement: { _ in
                        if callIndex == self.failureOnWindow {
                            throw self.makeRefusalError()
                        }
                        return RefinementWindowSchema(spans: [])
                    }
                )
            }
        )
    }

    private func nextCallIndex() -> Int {
        let idx = callIndex
        callIndex += 1
        return idx
    }

    nonisolated private func makeRefusalError() -> Error {
        #if canImport(FoundationModels)
        if #available(iOS 26.0, *) {
            let context = LanguageModelSession.GenerationError.Context(
                debugDescription: "May contain sensitive content"
            )
            let refusal = LanguageModelSession.GenerationError.Refusal(transcriptEntries: [])
            return LanguageModelSession.GenerationError.refusal(refusal, context)
        }
        #endif
        return NSError(
            domain: "FakeRefusalRuntime",
            code: 1,
            userInfo: [NSLocalizedDescriptionKey: "refusal"]
        )
    }
}

// MARK: - Fake runtime that always succeeds

private actor FakeSuccessRuntime {
    private let planCount: Int

    init(planCount: Int) {
        self.planCount = planCount
    }

    nonisolated var runtime: FoundationModelClassifier.Runtime {
        FoundationModelClassifier.Runtime(
            availabilityStatus: { _ in nil },
            contextSize: { 4096 },
            tokenCount: { _ in 4 },
            coarseSchemaTokenCount: { 8 },
            refinementSchemaTokenCount: { 16 },
            makeSession: {
                FoundationModelClassifier.Runtime.Session(
                    prewarm: { _ in },
                    respondCoarse: { _ in
                        CoarseScreeningSchema(disposition: .containsAd, support: nil)
                    },
                    respondRefinement: { _ in
                        RefinementWindowSchema(spans: [])
                    }
                )
            }
        )
    }
}
