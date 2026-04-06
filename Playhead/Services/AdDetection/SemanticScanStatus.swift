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
