// SemanticScanStatus.swift
// Phase 3 scan lifecycle and failure mapping for Foundation Models work.

import Foundation

#if canImport(FoundationModels)
import FoundationModels
#endif

enum SemanticScanStatus: String, Codable, Sendable, Hashable, CaseIterable {
    case queued
    case running
    case success
    case unavailable
    case unsupportedLocale
    case exceededContextWindow
    case decodingFailure
    case refusal
    case guardrailViolation
    case assetsUnavailable
    case rateLimited
    case thermalDeferred
    case cancelled
    case failedTransient
    // Cycle 4 H-1: permissive-bypass failure variants. These mirror
    // the standard-path `.refusal` / `.decodingFailure` /
    // `.exceededContextWindow` cases but preserve the permissive-path
    // distinction in the persisted `semantic_scan_results.status`
    // column. Raw values are stable strings so a row written today
    // decodes identically after future enum reordering.
    case permissiveRefusal = "permissive_refusal"
    case permissiveDecodingFailure = "permissive_decoding_failure"
    case permissiveContextOverflow = "permissive_context_overflow"
    // H1-FM: eu1 permissive retry succeeded but returned no ad spans.
    // Recorded so callers can account for every window in the plan.
    case noAds = "no_ads"

    /// Documents the recovery path for each status so backfill and future
    /// persistence code can make the same retry decision everywhere.
    var retryPolicy: SemanticScanRetryPolicy {
        switch self {
        case .queued, .running, .success, .unavailable, .unsupportedLocale:
            .none
        case .exceededContextWindow:
            .shrinkWindowAndRetryOnce
        case .decodingFailure:
            .simplifySchemaAndRetryOnce
        case .refusal, .guardrailViolation:
            .persistFailure
        case .assetsUnavailable:
            .deferUntilAssetsReady
        case .rateLimited:
            .backoffAndRetry
        case .thermalDeferred, .cancelled:
            .resumeFromCheckpoint
        case .failedTransient:
            .retryTransiently
        // Cycle 4 H-1: permissive failure variants. All three bypass
        // same-pass retry — the permissive path is already the
        // fallback the router uses after the standard @Generable path
        // would refuse. Re-running the permissive path in the same
        // pass would just reproduce the same failure. The shadow
        // retry observer picks these up on the next capability
        // transition, same as standard `.refusal`.
        case .permissiveRefusal, .permissiveDecodingFailure, .permissiveContextOverflow:
            .persistFailure
        case .noAds:
            .none
        }
    }

    static func from(error: Error) -> SemanticScanStatus {
        if error is CancellationError {
            return .cancelled
        }

        #if canImport(FoundationModels)
        // playhead-l3r2: iOS/macOS 27 throws the NEW `LanguageModelError`
        // type; iOS/macOS 26 threw `LanguageModelSession.GenerationError`.
        // Attempt the new cast first, then fall back to the legacy cast, so
        // refusal (permissive-fallback) and context-overflow (smart-shrink)
        // routing stays armed on BOTH OS generations. The two casts are for
        // disjoint types, so ordering only reflects the common runtime case.
        if #available(iOS 27.0, macOS 27.0, visionOS 27.0, watchOS 27.0, *),
           let languageModelError = error as? LanguageModelError {
            return from(languageModelError: languageModelError)
        }
        if #available(iOS 26.0, *),
           let generationError = error as? LanguageModelSession.GenerationError {
            return from(generationError: generationError)
        }
        #endif

        return .failedTransient
    }

    #if canImport(FoundationModels)
    @available(iOS 26.0, *)
    static func from(availability: SystemLanguageModel.Availability) -> SemanticScanStatus? {
        switch availability {
        case .available:
            nil
        case .unavailable(.modelNotReady):
            .assetsUnavailable
        case .unavailable:
            .unavailable
        }
    }

    @available(iOS 26.0, *)
    static func from(generationError: LanguageModelSession.GenerationError) -> SemanticScanStatus {
        switch generationError {
        case .exceededContextWindowSize:
            .exceededContextWindow
        case .assetsUnavailable:
            .assetsUnavailable
        case .guardrailViolation:
            .guardrailViolation
        case .unsupportedGuide:
            .decodingFailure
        case .unsupportedLanguageOrLocale:
            .unsupportedLocale
        case .decodingFailure:
            .decodingFailure
        case .rateLimited:
            .rateLimited
        case .concurrentRequests:
            .rateLimited
        case .refusal:
            .refusal
        @unknown default:
            .failedTransient
        }
    }

    /// iOS/macOS 27 renamed and restructured the thrown generation-failure
    /// type from `LanguageModelSession.GenerationError` to the top-level
    /// `LanguageModelError`. This mirrors `from(generationError:)` case-for-
    /// case so the same `SemanticScanStatus` (and therefore the same
    /// `retryPolicy`) is produced on iOS 27 as on iOS 26. playhead-l3r2.
    ///
    /// Cases with no legacy `GenerationError` analog are documented inline.
    /// Legacy cases with no `LanguageModelError` analog:
    ///   - `assetsUnavailable`: on iOS 27 asset readiness is surfaced through
    ///     `SystemLanguageModel.Availability.unavailable(.modelNotReady)`
    ///     (see `from(availability:)`), not through a thrown error.
    ///   - `concurrentRequests`: folded into `.rateLimited` in the new enum.
    @available(iOS 27.0, macOS 27.0, visionOS 27.0, watchOS 27.0, *)
    static func from(languageModelError: LanguageModelError) -> SemanticScanStatus {
        switch languageModelError {
        case .contextSizeExceeded:
            .exceededContextWindow
        case .rateLimited:
            .rateLimited
        case .guardrailViolation:
            .guardrailViolation
        case .refusal:
            .refusal
        case .unsupportedGenerationGuide:
            .decodingFailure
        case .unsupportedLanguageOrLocale:
            .unsupportedLocale
        case .unsupportedTranscriptContent:
            // No legacy analog. The transcript/prompt carried content the
            // model can't process; nearest existing status is the decoding /
            // unsupported-guide family (simplify-schema-and-retry-once).
            .decodingFailure
        case .unsupportedCapability:
            // No legacy analog. The requested model capability (e.g.
            // reasoning / tool-calling) isn't serviceable in this config;
            // retrying the same request won't help, so map to the no-retry
            // `.unavailable` status.
            .unavailable
        case .timeout:
            // No legacy analog. Transient by nature — allow the standard
            // transient retry.
            .failedTransient
        @unknown default:
            .failedTransient
        }
    }
    #endif
}

enum SemanticScanRetryPolicy: String, Codable, Sendable, Hashable, CaseIterable {
    case none
    case shrinkWindowAndRetryOnce
    case simplifySchemaAndRetryOnce
    case persistFailure
    case deferUntilAssetsReady
    case backoffAndRetry
    case resumeFromCheckpoint
    case retryTransiently
}
