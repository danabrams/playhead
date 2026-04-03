// FoundationModelExtractor.swift
// Layer 3 Foundation Models metadata extractor using guided generation.
//
// Uses Apple's FoundationModels framework for schema-bound extraction of
// advertiser, product, and evidence from transcript text. Guarded by
// #if canImport(FoundationModels) — compiles to a no-op on unsupported devices.
//
// Backfill only. Never on the hot path. Never the sole reason a skip fires.

import Foundation
import OSLog

#if canImport(FoundationModels)
import FoundationModels

// MARK: - Schema Types for Guided Generation

/// Schema-bound output for Foundation Models guided generation.
/// The model can only produce values conforming to this structure.
@Generable
struct AdMetadataSchema: Sendable {
    /// The advertiser or brand name mentioned in the transcript.
    @Guide(description: "The advertiser or brand name explicitly mentioned in the transcript evidence. Null if no advertiser is clearly stated.")
    var advertiser: String?

    /// The product or service being promoted.
    @Guide(description: "The specific product or service being promoted. Null if no product is clearly identified.")
    var product: String?

    /// The verbatim text that supports the extraction.
    @Guide(description: "The exact verbatim substring from the transcript that most directly supports identifying this as an ad. Keep under 150 characters.")
    var evidenceText: String

    /// Confidence that this is genuinely an ad (0.0 to 1.0).
    @Guide(description: "Confidence from 0.0 to 1.0 that the transcript evidence represents a genuine advertisement, not editorial content or organic mention.")
    var confidence: Double
}

// MARK: - FoundationModelExtractor

/// Extracts structured ad metadata using on-device Foundation Models.
/// Schema-bound: output is constrained to AdMetadataSchema fields.
/// Evidence-bound: only extracts what the transcript text supports.
struct FoundationModelExtractor: MetadataExtractor {

    private let logger = Logger(subsystem: "com.playhead", category: "FoundationModelExtractor")

    func extract(
        evidenceText: String,
        windowStartTime: Double,
        windowEndTime: Double
    ) async throws -> AdMetadata? {
        guard !evidenceText.isEmpty else { return nil }

        guard #available(iOS 26.0, *) else { return nil }

        let model = SystemLanguageModel.default
        guard model.isAvailable else {
            logger.info("Foundation model not available, skipping extraction")
            return nil
        }

        let session = LanguageModelSession()

        let prompt = Self.buildPrompt(
            evidenceText: evidenceText,
            windowStartTime: windowStartTime,
            windowEndTime: windowEndTime
        )

        do {
            let response = try await session.respond(
                to: prompt,
                generating: AdMetadataSchema.self
            )

            let schema = response.content
            let clampedConfidence = min(max(schema.confidence, 0.0), 1.0)

            logger.info("Extracted: advertiser=\(schema.advertiser ?? "nil"), product=\(schema.product ?? "nil"), confidence=\(clampedConfidence)")

            return AdMetadata(
                advertiser: schema.advertiser,
                product: schema.product,
                evidenceText: String(schema.evidenceText.prefix(200)),
                confidence: clampedConfidence,
                promptVersion: MetadataPromptVersion.current,
                source: "foundationModels"
            )
        } catch {
            logger.error("Foundation model extraction failed: \(error.localizedDescription)")
            throw error
        }
    }

    // MARK: - Prompt Construction

    /// Build the extraction prompt. Versioned via MetadataPromptVersion.
    /// Changes here require bumping MetadataPromptVersion.current.
    private static func buildPrompt(
        evidenceText: String,
        windowStartTime: Double,
        windowEndTime: Double
    ) -> String {
        """
        You are analyzing a podcast transcript segment that has been flagged as \
        a potential advertisement. The segment spans from \
        \(String(format: "%.1f", windowStartTime))s to \
        \(String(format: "%.1f", windowEndTime))s.

        Extract the advertiser name, product, and supporting evidence from \
        the transcript text below. Only extract information that is explicitly \
        stated in the text. If the advertiser or product cannot be clearly \
        identified from the text, leave them null.

        Transcript:
        \(evidenceText.prefix(1000))
        """
    }
}

#endif

// MARK: - Factory

/// Creates the appropriate MetadataExtractor based on device capabilities.
/// Checks canUseFoundationModels from CapabilitySnapshot to decide.
enum MetadataExtractorFactory {

    /// Returns a FoundationModelExtractor when available, FallbackExtractor otherwise.
    static func makeExtractor(snapshot: CapabilitySnapshot) -> MetadataExtractor {
        #if canImport(FoundationModels)
        if snapshot.canUseFoundationModels {
            return FoundationModelExtractor()
        }
        #endif
        return FallbackExtractor()
    }

    /// Check whether previously extracted metadata is stale and needs re-extraction.
    /// Stale when: prompt version changed or metadata was never extracted.
    static func needsReExtraction(
        currentPromptVersion: String?,
        currentSource: String?
    ) -> Bool {
        guard let version = currentPromptVersion else { return true }
        return version != MetadataPromptVersion.current
    }
}
