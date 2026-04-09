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
        }
    }

    static func from(error: Error) -> SemanticScanStatus {
        if error is CancellationError {
            return .cancelled
        }

        #if canImport(FoundationModels)
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
