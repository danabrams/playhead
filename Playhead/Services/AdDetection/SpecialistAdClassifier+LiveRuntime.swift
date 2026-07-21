// SpecialistAdClassifier+LiveRuntime.swift
// playhead-b6jq PR 3 (Phase B2): the live-runtime factory that plugs the
// on-device `SpecialistEngineProvider` into the model-agnostic
// `SpecialistAdClassifier.Runtime` seam that B1 documented
// (`SpecialistAdClassifier.swift:17-25`).
//
// Mirrors `FoundationModelClassifier.makeLiveRuntimeForShadow()`: a factory
// that returns a `Runtime` whose `makeSession()` hands out sessions backed by
// a live engine — without leaking CoreAI into the seam.
//
// # Phone gate + default-OFF
//
// The factory returns `Runtime?`. On device it builds a real runtime; on
// simulator (and any build without the engine) it returns `nil`, so PR 4's
// injection site can inject that `nil` and nothing changes there. This PR does
// NOT inject the runtime anywhere — it only makes it CONSTRUCTIBLE.

import Foundation
import OSLog

extension SpecialistAdClassifier {
    /// Build a live `Runtime` backed by the on-device CoreAILM specialist
    /// engine at `modelURL`.
    ///
    /// Single-flight: the factory constructs ONE `SpecialistEngineProvider`
    /// (hence one engine) and every `Session` it hands out shares that
    /// provider. The engine loads on-demand on the first `classify` and is
    /// reused across windows (KV `reset` per window) — the ~5.6s load is paid
    /// once, not per session.
    ///
    /// Each session's `classify` returns the RAW signal:
    /// `SpecialistVerdict(isAd: P(ad) >= 0.5, confidence: P(ad), adClass: "hostRead")`.
    /// The τ=0.7 skip threshold is applied DOWNSTREAM (PR 5's mark composer);
    /// the runtime deliberately does not bake it in.
    ///
    /// - Returns: A live `Runtime` on device, or `nil` on simulator / any build
    ///   without the engine (the phone gate). Callers treat `nil` as "no
    ///   specialist runtime available" and change nothing.
    static func makeLiveRuntime(
        modelURL: URL,
        logger: Logger = Logger(subsystem: "com.playhead", category: "SpecialistLiveRuntime")
    ) -> Runtime? {
        #if canImport(CoreAILanguageModels) && !targetEnvironment(simulator)
        // ONE provider (one engine) shared by every session this runtime hands
        // out — the single-flight guarantee lives in the provider actor.
        let provider = SpecialistEngineProvider(modelURL: modelURL, logger: logger)
        return Runtime(makeSession: {
            Runtime.Session(classify: { prompt in
                let probabilityOfAd = try await provider.classify(prompt: prompt)
                return SpecialistVerdict(
                    isAd: probabilityOfAd >= 0.5,
                    confidence: probabilityOfAd,
                    adClass: "hostRead"
                )
            })
        })
        #else
        _ = modelURL
        _ = logger
        return nil
        #endif
    }
}
