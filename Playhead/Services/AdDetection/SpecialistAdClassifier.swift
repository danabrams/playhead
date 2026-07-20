// SpecialistAdClassifier.swift
// playhead-dsbc (Phase B1): the model-agnostic seam for the distilled
// specialist ad classifier.
//
// # What this is
//
// Playhead's ad detection will eventually augment the Foundation Models (FM)
// coarse pass with a distilled specialist classifier (a fine-tuned
// Qwen3-0.6B, trained offline): the specialist marks candidate ads at high
// recall, FM disposes the residual, and deterministic signals own auto-skip.
//
// Phase B1 builds ONLY the model-agnostic SHADOW plumbing — a value-type of
// pure `@Sendable` closures that mirrors `FoundationModelClassifier.Runtime`.
// It carries no model and no framework dependency, so the app target keeps
// compiling and testing on the iOS Simulator.
//
// # Deliberately absent: the live runtime (Phase B2)
//
// The live, `CoreAILanguageModel`-backed runtime is Phase B2 (phone-gated)
// and is intentionally NOT in this file. B1 must not depend on CoreAI /
// CoreAIRuntime — those frameworks are unavailable on the iOS Simulator, and
// keeping this seam model-agnostic is a hard acceptance criterion. When B2
// arrives it will add a `makeLiveRuntime()` factory in a phone-gated file
// (mirroring `FoundationModelClassifier.makeLiveRuntimeForShadow()`), leaving
// this seam untouched.

import Foundation

// MARK: - SpecialistVerdict

/// A single classifier verdict for one transcript window.
///
/// Model-agnostic: it says nothing about how the verdict was produced, only
/// what it is. The shadow dispatcher persists this verbatim so downstream
/// (Phase B2+) harnesses can evaluate the specialist's marks without
/// re-running the model.
struct SpecialistVerdict: Sendable, Codable, Equatable {
    /// Whether the window is judged to contain ad content. High-recall by
    /// design — the specialist marks candidates; FM/deterministic signals
    /// dispose. In B1 nothing acts on this.
    let isAd: Bool

    /// Model confidence in the closed interval `0...1`. `init` clamps
    /// out-of-range inputs to be defensive: a model that emits a logit-ish
    /// value outside `[0, 1]` should never poison a downstream threshold
    /// comparison. Callers may treat pre-clamp out-of-range values as a
    /// model-contract violation.
    let confidence: Double

    /// Optional coarse class label (e.g. `"dai"`, `"hostRead"`). `nil` when
    /// the model does not emit one — the seam does not mandate a taxonomy in
    /// B1.
    let adClass: String?

    init(isAd: Bool, confidence: Double, adClass: String? = nil) {
        self.isAd = isAd
        self.confidence = min(1.0, max(0.0, confidence))
        self.adClass = adClass
    }
}

// MARK: - SpecialistAdClassifier

/// Namespace for the specialist classifier's model-agnostic types. Uninhabited
/// (an `enum` with no cases) — it exists only to scope `Runtime` alongside the
/// verdict type, mirroring how `FoundationModelClassifier.Runtime` is scoped.
enum SpecialistAdClassifier {

    /// Value-type of pure `@Sendable` closures — the seam a live model plugs
    /// into. Mirrors `FoundationModelClassifier.Runtime`: a `Session`
    /// value-type holding the per-call closure, and a `makeSession` factory
    /// that produces a fresh session (so a live implementation can confine a
    /// per-window model session behind the closure without leaking its
    /// concurrency model here).
    ///
    /// No CoreAI, no model, no I/O of its own — a test injects a deterministic
    /// closure; Phase B2 injects a `CoreAILanguageModel`-backed one.
    struct Runtime: Sendable {
        struct Session: Sendable {
            /// Classify a single assembled prompt (the window's transcript
            /// text) into a `SpecialistVerdict`. Throwing is a first-class
            /// outcome — the shadow dispatcher records the failure with an
            /// `errorTag` rather than dropping the datum.
            let classify: @Sendable (_ prompt: String) async throws -> SpecialistVerdict

            init(
                classify: @escaping @Sendable (_ prompt: String) async throws -> SpecialistVerdict
            ) {
                self.classify = classify
            }
        }

        /// Produce a fresh session. A live implementation makes a new model
        /// session per window (see the FM shadow dispatcher's per-window
        /// session lifetime); a test returns a static closure.
        let makeSession: @Sendable () async -> Session

        init(makeSession: @escaping @Sendable () async -> Session) {
            self.makeSession = makeSession
        }
    }
}
