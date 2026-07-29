import Foundation
import Testing
@testable import Playhead

#if canImport(FoundationModels)
import FoundationModels
#endif

@Suite("Semantic scan models")
struct SemanticScanStatusTests {

    @Test("ScanCohort is codable and hashable")
    func scanCohortRoundTrip() throws {
        let cohort = ScanCohort(
            promptLabel: "classify-v1",
            promptHash: "prompt-hash",
            schemaHash: "schema-hash",
            scanPlanHash: "scan-plan-hash",
            normalizationHash: "normalization-hash",
            osBuild: "iOS 26.4 (23A344)",
            locale: "en_US",
            appBuild: "42"
        )

        let data = try JSONEncoder().encode(cohort)
        let decoded = try JSONDecoder().decode(ScanCohort.self, from: data)

        #expect(decoded == cohort)
        #expect(Set([cohort, decoded]).count == 1)
    }

    @Test("retry policy matches the phase 3 contract")
    func retryPolicyContract() {
        #expect(SemanticScanStatus.exceededContextWindow.retryPolicy == .shrinkWindowAndRetryOnce)
        #expect(SemanticScanStatus.decodingFailure.retryPolicy == .simplifySchemaAndRetryOnce)
        #expect(SemanticScanStatus.assetsUnavailable.retryPolicy == .deferUntilAssetsReady)
        #expect(SemanticScanStatus.rateLimited.retryPolicy == .backoffAndRetry)
        #expect(SemanticScanStatus.thermalDeferred.retryPolicy == .resumeFromCheckpoint)
        #expect(SemanticScanStatus.cancelled.retryPolicy == .resumeFromCheckpoint)
        #expect(SemanticScanStatus.refusal.retryPolicy == .persistFailure)
        #expect(SemanticScanStatus.guardrailViolation.retryPolicy == .persistFailure)
    }

    // MARK: - playhead-qbib: per-window vs per-pass failure scope

    @Test("playhead-qbib: safety blocks and shape failures are window-scoped, device failures are pass-scoped")
    func failureScopeContract() {
        // Window-scoped: a property of THIS window's content or prompt. One
        // poisoned window must never end the pass.
        for status in [
            SemanticScanStatus.refusal,
            .guardrailViolation,
            .exceededContextWindow,
            .decodingFailure,
            .rateLimited,
            .permissiveRefusal,
            .permissiveDecodingFailure,
            .permissiveContextOverflow
        ] {
            #expect(status.failureScope == .window, "\(status.rawValue) must not abort the pass")
        }

        // Pass-scoped: a property of the device / model / session. Every
        // remaining window would fail identically.
        for status in [
            SemanticScanStatus.unavailable,
            .unsupportedLocale,
            .assetsUnavailable,
            .thermalDeferred,
            .cancelled,
            .failedTransient
        ] {
            #expect(status.failureScope == .pass, "\(status.rawValue) must stop the pass")
        }

        // Not failures at all. Every call site reads `scope == .window` and
        // aborts otherwise, so these must not be lumped in with `.pass`.
        for status in [SemanticScanStatus.success, .noAds, .queued, .running] {
            #expect(status.failureScope == .notAFailure, "\(status.rawValue) is not a failure")
        }

        // The mapping is total: no status is left unclassified.
        #expect(SemanticScanStatus.allCases.allSatisfy { status in
            SemanticScanFailureScope.allCases.contains(status.failureScope)
        })
    }

    @Test("playhead-qbib: only refusal and guardrail violation are safety blocks")
    func safetyBlockContract() {
        #expect(SemanticScanStatus.refusal.isSafetyBlock)
        #expect(SemanticScanStatus.guardrailViolation.isSafetyBlock)
        for status in SemanticScanStatus.allCases where status != .refusal && status != .guardrailViolation {
            #expect(!status.isSafetyBlock, "\(status.rawValue) is not a safety block")
        }
    }

    @Test("playhead-qbib: only success and noAds count as an examined window")
    func didExamineWindowContract() {
        #expect(SemanticScanStatus.success.didExamineWindow)
        #expect(SemanticScanStatus.noAds.didExamineWindow)
        for status in SemanticScanStatus.allCases where status != .success && status != .noAds {
            #expect(
                !status.didExamineWindow,
                "\(status.rawValue) produced no verdict — it must not count as scanned"
            )
        }
    }

    // MARK: - playhead-qbib: honest scanned-duration denominator

    /// DENOMINATOR: a refused window and a window screened clean both persist
    /// a row. Only the status distinguishes them, and this is the assertion
    /// that a consumer can act on that distinction.
    @Test("playhead-qbib: coverage separates scanned-clean from could-not-scan")
    func coverageSeparatesCleanFromUnscanned() {
        let rows = [
            makeCoverageRow(id: "w0", start: 0, end: 100, status: .success),
            makeCoverageRow(id: "w1", start: 100, end: 250, status: .guardrailViolation),
            makeCoverageRow(id: "w2", start: 250, end: 300, status: .noAds)
        ]

        let coverage = SemanticScanCoverage.compute(rows: rows)

        #expect(coverage.examinedSeconds == 150)
        #expect(coverage.unexaminedSeconds == 150)
        #expect(coverage.unexaminedRanges == [100 ... 250])
        #expect(coverage.accountedSeconds == 300)
        #expect(coverage.examinedFraction == 0.5)
        #expect(!coverage.isComplete)
    }

    /// The exact phone failure: rows stop at 1425.9s of a ~3578s episode and
    /// every row that WAS written looks fine. Only measuring against episode
    /// end reveals the truncation.
    @Test("playhead-qbib: coverage reports the unscanned tail through episode end")
    func coverageReportsTruncatedTail() {
        let rows = [
            makeCoverageRow(id: "w0", start: 0, end: 700, status: .success),
            makeCoverageRow(id: "w1", start: 700, end: 1425.9, status: .success)
        ]

        let coverage = SemanticScanCoverage.compute(rows: rows, episodeDuration: 3578)

        #expect(coverage.examinedSeconds == 1425.9)
        #expect(coverage.unexaminedRanges == [1425.9 ... 3578])
        #expect(!coverage.isComplete)
        // Without the episode duration the truncated run looks perfect —
        // which is precisely why the denominator has to be measured against
        // episode end and not against the rows the run happened to write.
        #expect(SemanticScanCoverage.compute(rows: rows).isComplete)
    }

    @Test("playhead-qbib: a window recovered by a smaller retry is not a coverage hole")
    func coverageSubtractsRecoveredRanges() {
        let rows = [
            // The whole window was blocked...
            makeCoverageRow(id: "w0", start: 0, end: 200, status: .refusal),
            // ...then both halves came back through the permissive retry.
            makeCoverageRow(id: "w0a", start: 0, end: 100, status: .success),
            makeCoverageRow(id: "w0b", start: 100, end: 200, status: .noAds)
        ]

        let coverage = SemanticScanCoverage.compute(rows: rows, episodeDuration: 200)

        #expect(coverage.unexaminedRanges.isEmpty)
        #expect(coverage.examinedSeconds == 200)
        #expect(coverage.isComplete)
    }

    @Test("playhead-qbib: coverage ignores passB rows and degenerate coordinates")
    func coverageIgnoresOtherPassesAndZeroWidthRows() {
        let rows = [
            makeCoverageRow(id: "a", start: 0, end: 100, status: .success),
            makeCoverageRow(id: "b", start: 0, end: 0, status: .refusal),
            makeCoverageRow(id: "c", start: 0, end: 100, status: .refusal, scanPass: "passB")
        ]

        let coverage = SemanticScanCoverage.compute(rows: rows)

        #expect(coverage.isComplete)
        #expect(coverage.examinedSeconds == 100)
    }

    /// playhead-pz32: a NO-WORK SENTINEL carries `status == .noAds` — whose
    /// `didExamineWindow` is `true` — and spans the WHOLE attempted transcript
    /// range, while meaning "no work was performed". Counting it turns a job that
    /// made zero FM calls into a fully-screened episode.
    @Test("playhead-pz32: a no-work sentinel is a coverage HOLE, not an examination")
    func coverageExcludesNoWorkSentinels() {
        let sentinel = makeCoverageRow(
            id: "sentinel",
            start: 0,
            end: 3578,
            status: .noAds,
            errorContext: "\(SemanticScanResult.noWorkSentinelErrorContextPrefix)emptySegments"
        )
        // The trap in one line: the STATUS says examined, the ROW does not.
        #expect(sentinel.status.didExamineWindow)
        #expect(sentinel.isNoWorkSentinel)
        #expect(!sentinel.didExamineWindow)

        let coverage = SemanticScanCoverage.compute(rows: [sentinel], episodeDuration: 3578)
        #expect(coverage.examinedSeconds == 0)
        #expect(coverage.unexaminedSeconds == 3578)
        #expect(coverage.unexaminedRanges == [0 ... 3578])
        #expect(!coverage.isComplete)

        // A real `.noAds` verdict alongside it still counts, so the exclusion is
        // the sentinel marker and not the `.noAds` status.
        let real = makeCoverageRow(id: "real", start: 0, end: 100, status: .noAds)
        #expect(real.didExamineWindow)
        let mixed = SemanticScanCoverage.compute(rows: [sentinel, real], episodeDuration: 3578)
        #expect(mixed.examinedSeconds == 100)
        #expect(mixed.unexaminedRanges == [100 ... 3578])
    }

    /// The static form is what a narrow SQL projection uses (it has raw column
    /// values, not a decoded row), so it must agree with the instance form and
    /// must treat an unrecognised persisted status string as NOT examined.
    @Test("playhead-pz32: the static didExamineWindow matches the row form")
    func staticDidExamineWindowMatchesRowForm() {
        for status in SemanticScanStatus.allCases {
            #expect(
                SemanticScanResult.didExamineWindow(status: status, errorContext: nil)
                    == makeCoverageRow(id: "r", start: 0, end: 1, status: status).didExamineWindow,
                "static/instance disagreement for \(status.rawValue)"
            )
            // The sentinel marker vetoes every status.
            #expect(!SemanticScanResult.didExamineWindow(
                status: status,
                errorContext: "\(SemanticScanResult.noWorkSentinelErrorContextPrefix)x"
            ))
        }
        // An unrecognised / missing persisted status string under-claims.
        #expect(!SemanticScanResult.didExamineWindow(status: nil, errorContext: nil))
        // An unrelated errorContext (a real failure's diagnostics) is not a veto.
        #expect(SemanticScanResult.didExamineWindow(status: .success, errorContext: "retry:1"))
    }

    private func makeCoverageRow(
        id: String,
        start: Double,
        end: Double,
        status: SemanticScanStatus,
        scanPass: String = "passA",
        errorContext: String? = nil
    ) -> SemanticScanResult {
        SemanticScanResult(
            id: id,
            analysisAssetId: "asset-qbib",
            windowFirstAtomOrdinal: 0,
            windowLastAtomOrdinal: 1,
            windowStartTime: start,
            windowEndTime: end,
            scanPass: scanPass,
            transcriptQuality: .good,
            disposition: status.didExamineWindow ? .noAds : .abstain,
            spansJSON: "[]",
            status: status,
            attemptCount: 1,
            errorContext: errorContext,
            inputTokenCount: nil,
            outputTokenCount: nil,
            latencyMs: 1,
            prewarmHit: false,
            scanCohortJSON: "{}",
            transcriptVersion: "tx-qbib"
        )
    }

    @Test("usability probe cache is keyed by OS build and boot epoch")
    func probeCacheKeys() {
        let suiteName = "SemanticScanStatusTests.\(UUID().uuidString)"
        let defaults = UserDefaults(suiteName: suiteName)!
        defer {
            defaults.removePersistentDomain(forName: suiteName)
        }

        FoundationModelsUsabilityProbe.cache(
            usable: true,
            userDefaults: defaults,
            osBuild: "iOS 26.4",
            bootEpochSeconds: 123
        )

        #expect(
            FoundationModelsUsabilityProbe.cachedUsability(
                userDefaults: defaults,
                osBuild: "iOS 26.4",
                bootEpochSeconds: 123
            ) == true
        )
        #expect(
            FoundationModelsUsabilityProbe.cachedUsability(
                userDefaults: defaults,
                osBuild: "iOS 26.5",
                bootEpochSeconds: 123
            ) == nil
        )
        #expect(
            FoundationModelsUsabilityProbe.cachedUsability(
                userDefaults: defaults,
                osBuild: "iOS 26.4",
                bootEpochSeconds: 456
            ) == nil
        )
    }

#if canImport(FoundationModels)
    @available(iOS 26.0, *)
    @Test("availability mapping covers all public unavailable reasons")
    func availabilityMapping() {
        #expect(SemanticScanStatus.from(availability: .available) == nil)
        #expect(SemanticScanStatus.from(availability: .unavailable(.deviceNotEligible)) == .unavailable)
        #expect(SemanticScanStatus.from(availability: .unavailable(.appleIntelligenceNotEnabled)) == .unavailable)
        #expect(SemanticScanStatus.from(availability: .unavailable(.modelNotReady)) == .assetsUnavailable)
    }

    @available(iOS 26.0, *)
    @Test("generation error mapping covers all published cases")
    func generationErrorMapping() {
        let context = LanguageModelSession.GenerationError.Context(debugDescription: "test")
        let refusal = LanguageModelSession.GenerationError.Refusal(transcriptEntries: [])

        let expectations: [(LanguageModelSession.GenerationError, SemanticScanStatus)] = [
            (.exceededContextWindowSize(context), .exceededContextWindow),
            (.assetsUnavailable(context), .assetsUnavailable),
            (.guardrailViolation(context), .guardrailViolation),
            (.unsupportedGuide(context), .decodingFailure),
            (.unsupportedLanguageOrLocale(context), .unsupportedLocale),
            (.decodingFailure(context), .decodingFailure),
            (.rateLimited(context), .rateLimited),
            (.concurrentRequests(context), .rateLimited),
            (.refusal(refusal, context), .refusal),
        ]

        for (error, expected) in expectations {
            #expect(SemanticScanStatus.from(generationError: error) == expected)
            #expect(SemanticScanStatus.from(error: error) == expected)
        }

        #expect(SemanticScanStatus.from(error: CancellationError()) == .cancelled)
        #expect(SemanticScanStatus.from(error: NSError(domain: "PlayheadTests", code: 7)) == .failedTransient)
    }

    // playhead-l3r2: iOS/macOS 27 throws the NEW `LanguageModelError` type
    // (not the legacy `LanguageModelSession.GenerationError`). Before the fix,
    // `from(error:)` only cast to `GenerationError`, so every iOS-27 refusal
    // fell through to `.failedTransient` — silently disarming the
    // permissive-fallback (keyed on `.refusal`) and smart-shrink retry (keyed
    // on `.exceededContextWindow`). These are the exact recovery mechanisms
    // that ship to the user's iOS-27 device. Every assertion here goes through
    // `from(error:)` (the production seam), so it fails at runtime on the
    // unfixed mapping.
    @available(iOS 27.0, macOS 27.0, visionOS 27.0, watchOS 27.0, *)
    @Test("language model error (iOS 27) mapping covers all published cases via from(error:)")
    func languageModelErrorMappingViaFromError() {
        let expectations: [(LanguageModelError, SemanticScanStatus)] = [
            (.contextSizeExceeded(.init(contextSize: 4096, tokenCount: 8192, debugDescription: "test")), .exceededContextWindow),
            (.rateLimited(.init(resetDate: nil, debugDescription: "test")), .rateLimited),
            (.guardrailViolation(.init(debugDescription: "test")), .guardrailViolation),
            (.refusal(.init(explanation: "test", debugDescription: "test")), .refusal),
            (.unsupportedGenerationGuide(.init(schemaName: "AdSchema", debugDescription: "test")), .decodingFailure),
            (.unsupportedLanguageOrLocale(.init(languageCode: Locale.LanguageCode("fr"), debugDescription: "test")), .unsupportedLocale),
            (.unsupportedTranscriptContent(.init(unsupportedContent: [], debugDescription: "test")), .decodingFailure),
            (.unsupportedCapability(.init(capability: .reasoning, debugDescription: "test")), .unavailable),
            (.timeout(.init(debugDescription: "test")), .failedTransient),
        ]

        for (error, expected) in expectations {
            // Direct mapping seam.
            #expect(SemanticScanStatus.from(languageModelError: error) == expected)
            // Production seam — the one that was disarmed on iOS 27.
            #expect(SemanticScanStatus.from(error: error) == expected)
        }
    }

    // playhead-l3r2: guard against a regression in the reverse direction — the
    // legacy iOS-26 `GenerationError` path must keep mapping refusal and
    // context-overflow correctly even after the new `LanguageModelError` cast
    // is added ahead of it in `from(error:)`.
    @available(iOS 26.0, *)
    @Test("legacy generation-error refusal + context overflow still map after the iOS-27 fix")
    func legacyGenerationErrorStillMapsAfterFix() {
        let context = LanguageModelSession.GenerationError.Context(debugDescription: "test")
        let refusal = LanguageModelSession.GenerationError.Refusal(transcriptEntries: [])

        #expect(SemanticScanStatus.from(error: LanguageModelSession.GenerationError.refusal(refusal, context)) == .refusal)
        #expect(SemanticScanStatus.from(error: LanguageModelSession.GenerationError.exceededContextWindowSize(context)) == .exceededContextWindow)
    }

    // playhead-cle1: iOS 27 split THREE more thrown-error responsibilities out
    // of the legacy `LanguageModelSession.GenerationError` into SEPARATE new
    // error types that l3r2's `LanguageModelError` bridge does NOT cover:
    //   - `GeneratedContent.ParsingError` (a struct)          → `.decodingFailure`
    //   - `LanguageModelSession.Error.concurrentRequests`     → `.rateLimited`
    //   - `SystemLanguageModel.Error.assetsUnavailable`       → `.assetsUnavailable`
    // Plus `LanguageModelSession.Error.transcriptMutationWhileResponding`,
    // which has no analog and (Dan-overridable) maps to `.failedTransient`.
    // Before the fix these all fell through `from(error:)` to
    // `.failedTransient`, disarming decode-simplify / backoff / defer recovery
    // on iOS 27. Every assertion here goes through the production `from(error:)`
    // seam, so it fails at runtime on the unfixed mapping.
    @available(iOS 27.0, macOS 27.0, visionOS 27.0, watchOS 27.0, *)
    @Test("iOS 27 ParsingError + session errors map via from(error:)")
    func newIOS27ErrorTypesMapViaFromError() {
        // GeneratedContent.ParsingError — parse/decode failure.
        let parsingError = GeneratedContent.ParsingError(
            rawContent: "{not json",
            debugDescription: "test"
        )
        #expect(SemanticScanStatus.from(error: parsingError) == .decodingFailure)
        #expect(SemanticScanStatus.from(parsingError: parsingError) == .decodingFailure)

        // LanguageModelSession.Error — session-level failures.
        #expect(
            SemanticScanStatus.from(error: LanguageModelSession.Error.concurrentRequests)
                == .rateLimited
        )
        #expect(SemanticScanStatus.from(sessionError: .concurrentRequests) == .rateLimited)
        // Documented Dan-overridable decision: no analog → transient retry.
        #expect(
            SemanticScanStatus.from(error: LanguageModelSession.Error.transcriptMutationWhileResponding)
                == .failedTransient
        )
        #expect(
            SemanticScanStatus.from(sessionError: .transcriptMutationWhileResponding) == .failedTransient
        )
    }

    // `SystemLanguageModel.Error` is unavailable on watchOS, so this seam is
    // exercised separately without the watchOS availability marker.
    @available(iOS 27.0, macOS 27.0, visionOS 27.0, *)
    @Test("iOS 27 SystemLanguageModel.Error.assetsUnavailable maps via from(error:)")
    func systemModelAssetsUnavailableMapsViaFromError() {
        let systemModelError = SystemLanguageModel.Error.assetsUnavailable(
            .init(debugDescription: "model assets not staged")
        )
        #expect(SemanticScanStatus.from(error: systemModelError) == .assetsUnavailable)
        #expect(SemanticScanStatus.from(systemModelError: systemModelError) == .assetsUnavailable)
    }
#endif
}
