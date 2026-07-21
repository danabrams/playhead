// playhead-b6jq PR 1: availability probe for the vendored CoreAILM
// specialist runtime (Vendor/coreai-models).
//
// This file is the ONLY place in the app allowed to import
// CoreAILanguageModels until the specialist engine lands (PR 2+). It is
// deliberately inert: no model, no inference, no resource loading — it
// exists so tests (and future gating code) can assert the link state of
// the vendored package without touching an engine.
//
// Simulator is compiled OUT on purpose: the specialist runtime is a
// device-only capability (mirrors the app's on-device FM posture), and
// the probe must report `false` under the FastTests simulator runs.

#if canImport(CoreAILanguageModels) && !targetEnvironment(simulator)
import CoreAILanguageModels
#endif

/// Reports whether the vendored CoreAILM specialist runtime is linked
/// into this build.
///
/// - `true`: device build with `Vendor/coreai-models`'s `CoreAILM`
///   product linked (FoundationModels-bridge files excluded by the
///   vendored manifest — see `Vendor/coreai-models/Package.swift`).
/// - `false`: simulator builds, and any build where the package is not
///   linkable.
enum SpecialistRuntimeAvailability {
    #if canImport(CoreAILanguageModels) && !targetEnvironment(simulator)
    static var isSpecialistRuntimeLinkable: Bool {
        // Reference a real (inert) module type so `true` proves the
        // module actually links — not merely that `canImport` passed at
        // compile time. `StopSequences` is a plain Sendable value type;
        // describing its metatype loads no model and runs no inference.
        _ = String(describing: StopSequences.self)
        return true
    }
    #else
    static var isSpecialistRuntimeLinkable: Bool { false }
    #endif
}
