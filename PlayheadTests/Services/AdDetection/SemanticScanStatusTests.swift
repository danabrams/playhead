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
