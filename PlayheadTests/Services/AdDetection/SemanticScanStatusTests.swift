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
#endif
}
