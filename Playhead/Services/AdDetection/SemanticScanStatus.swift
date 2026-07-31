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
    // playhead-8d5r: the inference call outlived its per-call deadline
    // (`FMInferenceDeadline.standard`) and was abandoned. NAMED rather than
    // folded into `.failedTransient` because the two need to stay
    // distinguishable in the persisted row: `.failedTransient` is "the model
    // returned an error we do not recognise", this is "the model returned
    // nothing at all within the budget". Collapsing them would make the very
    // measurement this bead exists to enable impossible — you could not tell a
    // bounded 180 s abandonment from the unbounded 1,664.9 s call it replaced.
    case inferenceTimeout = "inference_timeout"

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
        // playhead-8d5r: a timeout must NOT retry in the same pass. The call
        // already proved it cannot answer inside the budget, so an immediate
        // retry spends a SECOND full deadline to learn the same thing — and the
        // whole point of the deadline is to stop paying for calls that return
        // nothing. `.persistFailure` records the honest hole and lets the
        // existing shadow retry observer re-attempt on the next capability
        // transition, exactly as `.refusal` does.
        case .inferenceTimeout:
            .persistFailure
        }
    }

    /// playhead-qbib: does this status mean the window was actually
    /// EXAMINED and a verdict obtained ("we looked"), or that the attempt
    /// produced no verdict at all ("we could not look")?
    ///
    /// This is the load-bearing distinction for every scanned-duration
    /// denominator downstream: coverage %, precision measurement, and the
    /// playhead-0sro watermark invariants all divide by "how much audio did
    /// we actually screen". A refused or guardrailed window is NOT a window
    /// that was scanned and found clean — counting it as scanned silently
    /// shrinks the denominator and inflates every ratio computed from it.
    ///
    /// Only `.success` and `.noAds` are examinations. `.noAds` is the
    /// permissive path's "I looked and there is nothing here" verdict, which
    /// is a real examination even though it lives in the failed-window
    /// accounting list for count-completeness.
    var didExamineWindow: Bool {
        switch self {
        case .success, .noAds:
            true
        case .queued, .running, .unavailable, .unsupportedLocale,
             .exceededContextWindow, .decodingFailure, .refusal,
             .guardrailViolation, .assetsUnavailable, .rateLimited,
             .thermalDeferred, .cancelled, .failedTransient,
             .permissiveRefusal, .permissiveDecodingFailure,
             .permissiveContextOverflow, .inferenceTimeout:
            false
        }
    }

    /// playhead-qbib: how far does this failure generalize — is it a property
    /// of THIS window's content/prompt, or of the device/model/session?
    ///
    /// A `.window`-scoped failure must never end a pass. The phone evidence
    /// that opened this bead was a single mid-episode guardrail violation
    /// aborting the remaining coarse pass at 1425.9s of a ~3578s episode,
    /// leaving the postroll unscanned while the run still reported success.
    /// A `.pass`-scoped failure (model gone, assets missing, locale
    /// unsupported, device thermally deferred, task cancelled) would fail
    /// every remaining window identically, so continuing just burns FM calls
    /// and battery — those still stop the pass and resume from a checkpoint.
    ///
    /// Statuses that are not failures at all report `.notAFailure` rather than
    /// being lumped in with `.pass`. Call sites are shaped
    /// `if scope == .window { tolerate } else { abort }`, so bucketing
    /// `.success` or `.noAds` as `.pass` would make a non-failure abort a pass
    /// if one ever reached that switch.
    var failureScope: SemanticScanFailureScope {
        switch self {
        // playhead-8d5r: `.window`, deliberately. One slow window is not
        // evidence the device or model is unusable, and treating it as
        // `.pass` would throw away every window the pass had already banked —
        // the exact discarding playhead-bkhc fixed. A run of consecutive
        // timeouts IS device-level evidence, and `coarsePassA` escalates on
        // that separately (`consecutiveInferenceTimeoutAbortThreshold`)
        // rather than by mis-scoping a single one.
        case .exceededContextWindow, .decodingFailure, .refusal,
             .guardrailViolation, .rateLimited, .permissiveRefusal,
             .permissiveDecodingFailure, .permissiveContextOverflow,
             .inferenceTimeout:
            .window
        case .unavailable, .unsupportedLocale, .assetsUnavailable,
             .thermalDeferred, .cancelled, .failedTransient:
            .pass
        case .queued, .running, .success, .noAds:
            .notAFailure
        }
    }

    /// playhead-qbib: was this window blocked by Apple's safety layer rather
    /// than by a size/shape/rate problem? These are the two statuses the
    /// coarse pass retries through `PermissiveAdClassifier`, whose
    /// `.permissiveContentTransformations` guardrails are the documented
    /// mitigation for exactly this class of block.
    var isSafetyBlock: Bool {
        self == .refusal || self == .guardrailViolation
    }

    static func from(error: Error) -> SemanticScanStatus {
        // playhead-8d5r: check the deadline error FIRST. It is a distinct type
        // from `CancellationError`, so ordering is not a correctness
        // requirement — but the two are adjacent in meaning and the order
        // documents that a per-call deadline is NOT a cancellation. See
        // `FMInferenceTimeoutError`.
        if error is FMInferenceTimeoutError {
            return .inferenceTimeout
        }
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

/// playhead-qbib: blast radius of a scan failure. See
/// `SemanticScanStatus.failureScope` for the per-status mapping and the
/// rationale.
enum SemanticScanFailureScope: String, Codable, Sendable, Hashable, CaseIterable {
    /// The failure is a property of this window. Record it with honest
    /// coordinates and keep scanning the rest of the episode.
    case window
    /// The failure is a property of the device, model, or session. Stop the
    /// pass with partial results and resume from a checkpoint.
    case pass
    /// Not a failure — a lifecycle or success status. Never reachable from an
    /// error mapping; modelled explicitly so a non-failure can never be
    /// mistaken for a reason to abort.
    case notAFailure
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
