// FoundationModelClassifier.swift
// Minimum viable Foundation Models coarse scan for ad detection. Given a
// chunk of transcript text, asks the on-device language model whether it
// is commercial content (sponsor read, cross-promo, etc.) or editorial
// show content. This is the Phase 3.1 de-risking implementation — one
// function, no persistence, no zoom/refinement passes, no evidence
// grounding. Just: does FM catch what lexical scanning cannot?

import Foundation
import OSLog
#if canImport(FoundationModels)
import FoundationModels
#endif

// MARK: - Output schema

/// The commercial-intent classification produced by the FM coarse scan.
#if canImport(FoundationModels)
@Generable
struct CoarseScanResult: Sendable {
    @Guide(description: "The primary intent of this transcript segment. 'commercial' = sponsor read, ad, cross-promo, or any content primarily promoting a product, service, brand, or other podcast/show. 'editorial' = actual show content, interview, commentary, or storytelling. 'mixed' = a transition or brief mention that is neither purely commercial nor purely editorial.")
    var intent: CommercialIntent

    @Guide(description: "Confidence in the classification from 0.0 (uncertain) to 1.0 (certain). Use values below 0.5 when the segment is ambiguous.")
    var confidence: Double

    @Guide(description: "A brief (one-sentence) reason for the classification. If commercial, name the advertiser or promoted entity if identifiable. If editorial, name the topic briefly.")
    var reason: String
}

@Generable
enum CommercialIntent: String, Sendable {
    case commercial
    case editorial
    case mixed
}
#endif

// MARK: - Classifier

/// Public, FM-availability-aware result type used outside the
/// `canImport(FoundationModels)` guard.
struct FMCoarseScanOutput: Sendable {
    enum Intent: String, Sendable {
        case commercial
        case editorial
        case mixed
        case unavailable  // FM not available on this device
    }
    let intent: Intent
    let confidence: Double
    let reason: String
    /// Wall clock time in milliseconds for the FM call.
    let latencyMillis: Double
}

/// Minimum viable coarse FM classifier. Takes a transcript segment text
/// and returns a commercial-intent classification. No persistence, no
/// multi-pass, no grounding. Phase 3.1 proof of concept only.
struct FoundationModelClassifier: Sendable {

    private let logger = Logger(subsystem: "com.playhead", category: "FoundationModelClassifier")

    /// Run the coarse scan on a single transcript segment.
    /// Returns `.unavailable` if FM is not available on this device / OS.
    func coarse(segmentText: String) async throws -> FMCoarseScanOutput {
        let clock = ContinuousClock()
        let start = clock.now

        #if canImport(FoundationModels)
        guard #available(iOS 26.0, *) else {
            return FMCoarseScanOutput(intent: .unavailable, confidence: 0, reason: "iOS < 26", latencyMillis: 0)
        }

        let model = SystemLanguageModel.default
        if let availabilityStatus = SemanticScanStatus.from(availability: model.availability) {
            return FMCoarseScanOutput(intent: .unavailable, confidence: 0, reason: availabilityStatus.rawValue, latencyMillis: 0)
        }
        guard model.supportsLocale() else {
            return FMCoarseScanOutput(intent: .unavailable, confidence: 0, reason: SemanticScanStatus.unsupportedLocale.rawValue, latencyMillis: 0)
        }
        guard await FoundationModelsUsabilityProbe.probeIfNeeded(logger: logger) else {
            return FMCoarseScanOutput(intent: .unavailable, confidence: 0, reason: "foundationModelsProbeFailed", latencyMillis: 0)
        }

        let session = LanguageModelSession(model: model)
        let prompt = Self.buildPrompt(segmentText: segmentText)

        let response: CoarseScanResult
        do {
            let fullResponse = try await session.respond(to: prompt, generating: CoarseScanResult.self)
            response = fullResponse.content
        } catch {
            logger.error("Coarse scan failed: \(error.localizedDescription)")
            throw error
        }

        let elapsed = clock.now - start
        let elapsedMs = Double(elapsed.components.attoseconds) / 1e15 +
                        Double(elapsed.components.seconds) * 1000.0

        let intent: FMCoarseScanOutput.Intent
        switch response.intent {
        case .commercial: intent = .commercial
        case .editorial:  intent = .editorial
        case .mixed:      intent = .mixed
        }

        return FMCoarseScanOutput(
            intent: intent,
            confidence: min(max(response.confidence, 0), 1),
            reason: response.reason,
            latencyMillis: elapsedMs
        )
        #else
        return FMCoarseScanOutput(intent: .unavailable, confidence: 0, reason: "FoundationModels framework not available", latencyMillis: 0)
        #endif
    }

    // MARK: - Prompt

    private static func buildPrompt(segmentText: String) -> String {
        // Keep under ~1500 chars to be safe with token budgets.
        let truncated = String(segmentText.prefix(1200))
        return """
        You are analyzing a 10-30 second segment from a podcast transcript. \
        Classify whether this segment is commercial content (advertisement, \
        sponsor read, cross-promotion for another podcast or show, product \
        promotion with URL or promo code) or editorial content (the host \
        talking about the show's actual topic, interviewing a guest, \
        narrating a story, discussing news or opinion).

        Important guidance:
        - A segment that mentions a brand or product in passing while \
          telling a personal story is usually editorial.
        - A segment that promotes another podcast ("check out [show name]", \
          "listen wherever you get your podcasts") is commercial.
        - A segment with a URL + call to action ("go to X dot com", \
          "use code X") is almost always commercial.
        - A segment that is structural show information ("call our show at \
          X dot com slash call") is editorial — it's show structure, not a \
          sponsor.

        Transcript segment:
        \(truncated)
        """
    }
}
