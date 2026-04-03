// MetadataExtractor.swift
// Layer 3 of the ad detection pipeline: optional Foundation Models enrichment.
//
// Extracts structured metadata (advertiser, product, evidence) from transcript
// text within detected ad windows. Schema-bound guided generation only —
// no free-form output. Runs in backfill path, never hot path.
//
// NEVER the primary classifier. Foundation Models only enriches banners and
// arbitrates borderline cases. If unavailable, banner copy degrades gracefully.

import Foundation
import OSLog

// MARK: - Extracted Metadata

/// Structured metadata extracted from an ad window's transcript evidence.
/// All fields are evidence-bound: only populated when the transcript text
/// within the detected window supports them.
struct AdMetadata: Sendable, Codable, Equatable {
    /// The advertiser or brand name found in the transcript.
    let advertiser: String?
    /// The product or service being promoted.
    let product: String?
    /// The verbatim transcript text that supports this extraction.
    let evidenceText: String
    /// Confidence of the extraction (0.0...1.0).
    let confidence: Double
    /// Which prompt version produced this extraction.
    let promptVersion: String
    /// Source of the metadata ("foundationModels" or "fallback").
    let source: String
}

// MARK: - MetadataExtractor Protocol

/// Protocol for metadata extraction from transcript evidence.
/// Implementations must be safe to call from any isolation domain.
protocol MetadataExtractor: Sendable {
    /// Extract structured metadata from the transcript text within an ad window.
    ///
    /// - Parameters:
    ///   - evidenceText: The transcript text from the detected ad window.
    ///   - windowStartTime: Start time of the ad window in episode seconds.
    ///   - windowEndTime: End time of the ad window in episode seconds.
    /// - Returns: Extracted metadata, or nil if extraction fails entirely.
    func extract(
        evidenceText: String,
        windowStartTime: Double,
        windowEndTime: Double
    ) async throws -> AdMetadata?
}

// MARK: - Prompt Versioning

/// Tracks prompt versions for cache invalidation. When the prompt changes,
/// previously extracted metadata is stale and should be re-extracted.
enum MetadataPromptVersion {
    /// Current prompt version. Bump this when changing the extraction prompt.
    static let current = "metadata-v1"
}
