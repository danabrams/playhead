// FallbackExtractor.swift
// Fallback metadata extractor when Foundation Models is unavailable.
//
// Produces generic metadata from lexical signals already present in the
// transcript evidence. No ML inference — pure string matching.
// Banner copy degrades to "Ad · [evidence snippet]" when this is active.

import Foundation
import OSLog

// MARK: - FallbackExtractor

/// Extracts best-effort metadata using lexical heuristics only.
/// Active when Foundation Models is unavailable or the device is throttled.
struct FallbackExtractor: MetadataExtractor {

    private let logger = Logger(subsystem: "com.playhead", category: "FallbackExtractor")

    func extract(
        evidenceText: String,
        windowStartTime: Double,
        windowEndTime: Double
    ) async throws -> AdMetadata? {
        guard !evidenceText.isEmpty else { return nil }

        let normalized = evidenceText.lowercased()

        // Try to pull an advertiser from "brought to you by X" / "sponsored by X" patterns.
        let advertiser = extractAdvertiser(from: normalized)

        // Try to pull a product from "try X" / "check out X" patterns.
        let product = extractProduct(from: normalized)

        // Truncate evidence for storage (keep first 200 chars).
        let trimmedEvidence = String(evidenceText.prefix(200))

        let hasAnySignal = advertiser != nil || product != nil
        let confidence = hasAnySignal ? 0.3 : 0.1

        logger.debug("Fallback extraction: advertiser=\(advertiser ?? "nil"), product=\(product ?? "nil")")

        return AdMetadata(
            advertiser: advertiser,
            product: product,
            evidenceText: trimmedEvidence,
            confidence: confidence,
            promptVersion: MetadataPromptVersion.current,
            source: "fallback"
        )
    }

    // MARK: - Heuristic Extraction

    /// Extract advertiser from common sponsor patterns.
    /// Looks for "brought to you by X" or "sponsored by X" and grabs the
    /// next 1-3 capitalized words.
    private func extractAdvertiser(from text: String) -> String? {
        let patterns = [
            #"(?:brought to you by|sponsored by|thanks to|a word from)\s+([a-z][a-z\s]{1,30}?)(?:\s*[,.\-!]|\s+(?:who|where|they|the|a|with|and|is|are|use|go|head|check|visit))"#,
        ]

        for patternString in patterns {
            guard let regex = try? NSRegularExpression(
                pattern: patternString,
                options: [.caseInsensitive]
            ) else { continue }

            let nsText = text as NSString
            let fullRange = NSRange(location: 0, length: nsText.length)

            if let match = regex.firstMatch(in: text, range: fullRange),
               match.numberOfRanges > 1 {
                let captured = nsText.substring(with: match.range(at: 1))
                    .trimmingCharacters(in: .whitespaces)
                if !captured.isEmpty {
                    return capitalizeWords(captured)
                }
            }
        }

        return nil
    }

    /// Extract product from common CTA patterns.
    private func extractProduct(from text: String) -> String? {
        let patterns = [
            #"(?:try|check out|go to|visit|head to)\s+([a-z][a-z\s]{1,30}?)(?:\s*[,.\-!]|\s+(?:today|now|for|and|to|it|they|dot|com))"#,
        ]

        for patternString in patterns {
            guard let regex = try? NSRegularExpression(
                pattern: patternString,
                options: [.caseInsensitive]
            ) else { continue }

            let nsText = text as NSString
            let fullRange = NSRange(location: 0, length: nsText.length)

            if let match = regex.firstMatch(in: text, range: fullRange),
               match.numberOfRanges > 1 {
                let captured = nsText.substring(with: match.range(at: 1))
                    .trimmingCharacters(in: .whitespaces)
                if !captured.isEmpty {
                    return capitalizeWords(captured)
                }
            }
        }

        return nil
    }

    private func capitalizeWords(_ text: String) -> String {
        text.split(separator: " ")
            .map { $0.prefix(1).uppercased() + $0.dropFirst() }
            .joined(separator: " ")
    }
}
