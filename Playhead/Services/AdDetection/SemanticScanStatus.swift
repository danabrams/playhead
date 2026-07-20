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
        // playhead-cle1: iOS/macOS 27 also split THREE responsibilities the
        // legacy `GenerationError` carried into SEPARATE new error types that
        // `LanguageModelError` does NOT cover. Each is bridged by its own
        // helper below. The casts are for disjoint types, so ordering among
        // them is immaterial; they all precede the legacy iOS-26 cast so the
        // iOS-27 shapes win on iOS 27.
        if #available(iOS 27.0, macOS 27.0, visionOS 27.0, watchOS 27.0, *),
           let parsingError = error as? GeneratedContent.ParsingError {
            return from(parsingError: parsingError)
        }
        if #available(iOS 27.0, macOS 27.0, visionOS 27.0, watchOS 27.0, *),
           let sessionError = error as? LanguageModelSession.Error {
            return from(sessionError: sessionError)
        }
        // `SystemLanguageModel.Error` is unavailable on watchOS, so its guard
        // omits watchOS (matching the SDK type's own availability).
        if #available(iOS 27.0, macOS 27.0, visionOS 27.0, *),
           let systemModelError = error as? SystemLanguageModel.Error {
            return from(systemModelError: systemModelError)
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
    ///
    /// Three legacy `GenerationError` cases have NO `LanguageModelError`
    /// analog — iOS 27 moved them to *separate* new error types. As of
    /// playhead-cle1 those are bridged by the dedicated helpers below (each
    /// wired into `from(error:)`), so they no longer fall through to
    /// `.failedTransient` on iOS 27:
    ///   - `decodingFailure` → `GeneratedContent.ParsingError` →
    ///     `from(parsingError:)`.
    ///   - `concurrentRequests` → `LanguageModelSession.Error.concurrentRequests`
    ///     → `from(sessionError:)`.
    ///   - `assetsUnavailable` → `SystemLanguageModel.Error.assetsUnavailable`
    ///     (a thrown error on iOS 27) → `from(systemModelError:)`, in addition
    ///     to the pre-flight
    ///     `SystemLanguageModel.Availability.unavailable(.modelNotReady)`
    ///     signal still handled by `from(availability:)`.
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

    /// iOS/macOS 27 moved parse/decode failures out of
    /// `GenerationError.decodingFailure` / `.unsupportedGuide` and into the
    /// top-level `GeneratedContent.ParsingError` (a *struct*, not an enum —
    /// there is nothing to switch on). Maps to `.decodingFailure` so the
    /// `simplifySchemaAndRetryOnce` recovery and the refinement graceful-
    /// abandon path stay armed on iOS 27, exactly as the legacy
    /// `GenerationError.decodingFailure` → `.decodingFailure` mapping does on
    /// iOS 26. playhead-cle1.
    @available(iOS 27.0, macOS 27.0, visionOS 27.0, watchOS 27.0, *)
    static func from(parsingError: GeneratedContent.ParsingError) -> SemanticScanStatus {
        .decodingFailure
    }

    /// iOS/macOS 27 introduced `LanguageModelSession.Error` for session-level
    /// failures that the legacy `GenerationError` either carried differently
    /// or did not model at all. playhead-cle1.
    @available(iOS 27.0, macOS 27.0, visionOS 27.0, watchOS 27.0, *)
    static func from(sessionError: LanguageModelSession.Error) -> SemanticScanStatus {
        switch sessionError {
        case .concurrentRequests:
            // Matches the legacy `GenerationError.concurrentRequests` →
            // `.rateLimited` mapping (and its `backoffAndRetry` recovery).
            .rateLimited
        case .transcriptMutationWhileResponding:
            // PRODUCT DECISION (playhead-cle1 — DAN-OVERRIDABLE): this case has
            // NO legacy `GenerationError` analog and no dedicated
            // `SemanticScanStatus`. It signals that the session's transcript
            // was mutated while a response was in flight — a transient
            // client-side race with no special recovery — so it takes the
            // ordinary transient retry (`.retryTransiently`). If a distinct
            // status/recovery is ever wanted, change it here.
            .failedTransient
        @unknown default:
            .failedTransient
        }
    }

    /// iOS/macOS 27 can now THROW model-asset unavailability as
    /// `SystemLanguageModel.Error.assetsUnavailable`, in addition to the
    /// pre-flight `SystemLanguageModel.Availability.unavailable(.modelNotReady)`
    /// signal that `from(availability:)` still handles. Maps to
    /// `.assetsUnavailable` so the `deferUntilAssetsReady` recovery stays
    /// armed. `SystemLanguageModel.Error` is unavailable on watchOS, so this
    /// helper's availability omits watchOS (matching the SDK type).
    /// playhead-cle1.
    @available(iOS 27.0, macOS 27.0, visionOS 27.0, *)
    static func from(systemModelError: SystemLanguageModel.Error) -> SemanticScanStatus {
        switch systemModelError {
        case .assetsUnavailable:
            .assetsUnavailable
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
