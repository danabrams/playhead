// SpecialistEngineProvider.swift
// playhead-b6jq PR 3 (Phase B2): the phone-gated, FM-free on-device inference
// engine for the distilled specialist ad classifier.
//
// # What this is
//
// An `actor` that owns ONE CoreAILM inference engine and turns an assembled
// transcript-window prompt into `P(ad)` via a single forward pass +
// first-token softmax. It drives the CoreAILanguageModels engine DIRECTLY
// (`EngineFactory` -> `CoreAISequentialEngine`), bypassing the FoundationModels
// bridge that dyld-crashes on shipped seeds — the proven phaseB blueprint
// (`coreai-spike/phaseB/PhaseBProbe/Sources/App.swift`).
//
// # Phone gate
//
// The engine internals compile ONLY on device
// (`#if canImport(CoreAILanguageModels) && !targetEnvironment(simulator)`); the
// CoreAI framework is absent from the simulator SDK. A small always-compiled
// shell (init / `classify` / `release` / typed errors) lets callers use the
// provider from ungated code — on simulator `classify` throws
// `.runtimeUnavailable` and nothing loads.
//
// # Lifecycle contract
//
//   - single-flight: at most ONE engine instance per provider (and PR 4 injects
//     ONE provider app-wide via `makeLiveRuntime`), guarded by an in-flight
//     load `Task` so concurrent first-calls share one load.
//   - load-on-demand: the ~5.6s bundle+engine+tokenizer+warmup load happens on
//     the first `classify` of a session, not at construction.
//   - classify: one forward pass, `includeLogits`, greedy/deterministic, KV
//     `reset()` between windows, first-token softmax over `329`/`1921`.
//   - release: drop the engine to free ~202MB when the scan/session ends or on
//     cancellation.

import Foundation
import OSLog

#if canImport(CoreAILanguageModels) && !targetEnvironment(simulator)
import CoreAILanguageModels
import Tokenizers
#endif

// MARK: - Errors (always compiled)

/// Typed failures from the specialist engine. Always compiled so ungated
/// callers can pattern-match without a phone gate.
enum SpecialistEngineError: Error, LocalizedError, Equatable {
    /// The engine path is compiled out (simulator, or a build without CoreAI).
    case runtimeUnavailable
    /// Bundle / engine / tokenizer load failed. Payload is a stringified cause.
    case modelLoadFailed(String)
    /// The forward pass returned no usable logits (empty, or the label indices
    /// fell outside the vocab) — cannot compute `P(ad)`.
    case noLogits

    var errorDescription: String? {
        switch self {
        case .runtimeUnavailable:
            return "Specialist runtime is unavailable in this build (simulator or no CoreAI)."
        case .modelLoadFailed(let reason):
            return "Specialist model failed to load: \(reason)"
        case .noLogits:
            return "Specialist forward pass returned no usable logits."
        }
    }
}

// MARK: - SpecialistEngineProvider

/// Owns one on-device specialist inference engine and classifies assembled
/// prompts into `P(ad)`. See file header for the lifecycle contract.
actor SpecialistEngineProvider {
    private let modelURL: URL
    private let logger: Logger

    #if canImport(CoreAILanguageModels) && !targetEnvironment(simulator)
    /// A fully loaded, warmed engine + its tokenizer. `Sendable` (engine and
    /// tokenizer both refine `Sendable`), so it crosses the single-flight
    /// `Task` boundary cleanly.
    private struct Loaded {
        let engine: any InferenceEngine
        let tokenizer: any Tokenizer
        let vocabSize: Int
    }
    /// The loaded engine once ready. `nil` before first load / after `release`.
    private var loaded: Loaded?
    /// In-flight single-flight load. Concurrent first-`classify` callers await
    /// this same `Task` rather than each starting a second ~202MB load.
    private var loadTask: Task<Loaded, Error>?
    #endif

    init(
        modelURL: URL,
        logger: Logger = Logger(subsystem: "com.playhead", category: "SpecialistEngineProvider")
    ) {
        self.modelURL = modelURL
        self.logger = logger
    }

    // MARK: - Classify

    /// Classify one assembled prompt (the window's transcript text) into
    /// `P(ad)` in `0...1`. Loads the engine on first call (single-flight).
    ///
    /// - Throws: `SpecialistEngineError.runtimeUnavailable` on simulator,
    ///   `.modelLoadFailed` if the engine cannot load, `.noLogits` if the
    ///   forward pass yields no usable logits.
    func classify(prompt: String) async throws -> Double {
        #if canImport(CoreAILanguageModels) && !targetEnvironment(simulator)
        let engineState = try await ensureLoaded()
        let promptTokens = try Self.promptTokenIds(text: prompt, tokenizer: engineState.tokenizer)

        // Fresh KV per window — each classify is an independent judgment.
        try await engineState.engine.reset()

        // ONE forward pass. Greedy + maxTokens:1 + includeLogits is the
        // calibrated first-token-softmax path (never free-generation).
        var stepLogits: [LogitsScalarType]?
        for try await output in try await engineState.engine.generate(
            with: promptTokens,
            samplingConfiguration: .greedy,
            inferenceOptions: InferenceOptions(maxTokens: 1, includeLogits: true)
        ) {
            stepLogits = output.logits
            break
        }

        guard let logits = stepLogits, !logits.isEmpty else {
            throw SpecialistEngineError.noLogits
        }
        let adID = SpecialistFirstTokenSoftmax.adTokenID
        let notID = SpecialistFirstTokenSoftmax.notTokenID
        guard adID < logits.count, notID < logits.count else {
            throw SpecialistEngineError.noLogits
        }
        // Read ONLY the two label logits (vocab is ~152k; converting the whole
        // vector to Double would be wasteful). `LogitsScalarType` is Float16 on
        // device; widen via Float to avoid a lossy direct Float16->Double.
        let adLogit = Double(Float(logits[adID]))
        let notLogit = Double(Float(logits[notID]))
        guard adLogit.isFinite, notLogit.isFinite else {
            throw SpecialistEngineError.noLogits
        }
        return SpecialistFirstTokenSoftmax.probabilityOfAd(adLogit: adLogit, notLogit: notLogit)
        #else
        _ = prompt
        throw SpecialistEngineError.runtimeUnavailable
        #endif
    }

    // MARK: - Release

    /// Drop the loaded engine to free ~202MB. Idempotent; safe to call when
    /// nothing is loaded. Call when the scan/session ends or on cancellation.
    func release() {
        #if canImport(CoreAILanguageModels) && !targetEnvironment(simulator)
        loadTask?.cancel()
        loadTask = nil
        // Dropping the last strong reference deinits the engine and frees its
        // GPU/ANE buffers (~202MB in the phaseB probe).
        loaded = nil
        logger.debug("SpecialistEngineProvider released engine")
        #endif
    }

    // MARK: - Load (phone-gated)

    #if canImport(CoreAILanguageModels) && !targetEnvironment(simulator)
    /// Return the loaded engine, loading it exactly once. Single-flight: the
    /// first caller creates the load `Task`; concurrent callers await it. A
    /// failed load clears `loadTask` so a later `classify` can retry.
    private func ensureLoaded() async throws -> Loaded {
        if let loaded { return loaded }
        if let loadTask { return try await loadTask.value }

        let url = modelURL
        let log = logger
        let task = Task<Loaded, Error> {
            try await Self.loadEngine(modelURL: url, logger: log)
        }
        loadTask = task
        do {
            let result = try await task.value
            loaded = result
            loadTask = nil
            return result
        } catch {
            loadTask = nil
            throw error
        }
    }

    /// The minimal direct-engine load sequence — the phaseB B2 blueprint
    /// (`App.swift:141-172`), verbatim in ordering:
    ///   1. `LanguageBundle(at:)`
    ///   2. build `ModelConfig` (name/tokenizer/vocab/ctx/source/serializedModel/function)
    ///   3. `EngineFactory.createEngine(...)` forcing the `coreai-sequential`
    ///      variant (the auto-detected pipelined engine does GPU-side sampling
    ///      and throws on `includeLogits`, which we require).
    ///   4. `bundle.loadTokenizer()`
    ///   5. `engine.warmup(queryLength: 1, sampling: nil)`
    private static func loadEngine(modelURL: URL, logger: Logger) async throws -> Loaded {
        do {
            let bundle = try LanguageBundle(at: modelURL)
            let config = ModelConfig(
                name: bundle.name,
                tokenizer: bundle.tokenizer,
                vocabSize: bundle.vocabSize,
                maxContextLength: bundle.maxContextLength,
                source: ModelSource(hfModelId: bundle.tokenizer, modelDefinition: .pyTorch),
                serializedModel: [bundle.modelAssetPath],
                function: "main"
            )
            let configData = try JSONEncoder().encode(config)
            let engine = try await EngineFactory.createEngine(
                config: configData,
                modelURL: try bundle.requireModelURL(for: "main"),
                options: EngineOptions(variant: "coreai-sequential", kvCacheStrategy: .auto)
            )
            let tokenizer = try await bundle.loadTokenizer()
            try await engine.warmup(queryLength: 1, sampling: nil)
            logger.log(
                "specialist engine loaded: name=\(bundle.name, privacy: .public) vocab=\(bundle.vocabSize, privacy: .public) maxCtx=\(bundle.maxContextLength, privacy: .public)"
            )
            return Loaded(engine: engine, tokenizer: tokenizer, vocabSize: bundle.vocabSize)
        } catch {
            throw SpecialistEngineError.modelLoadFailed(String(describing: error))
        }
    }

    /// Build the fine-tune chat-template prompt with thinking suppressed —
    /// byte-identical to the phaseB spike (`App.swift:235-243`) and the FT prep
    /// (`coreai-spike/ft/prep_data.py`).
    private static func promptTokenIds(text: String, tokenizer: any Tokenizer) throws -> [Int32] {
        let messages: [[String: any Sendable]] = [
            ["role": "system", "content": systemPrompt],
            ["role": "user", "content": userMessage(for: text)],
        ]
        let ids = try tokenizer.applyChatTemplate(
            messages: messages,
            tools: nil,
            additionalContext: ["enable_thinking": false]
        )
        return ids.map(Int32.init)
    }
    #endif
}

// MARK: - Fine-tune prompt (pure; always compiled)

extension SpecialistEngineProvider {
    /// The fine-tune system prompt, verbatim from the training data
    /// (`coreai-spike/ft/prep_data.py`) and the phaseB spike
    /// (`App.swift:18-23`). Always compiled (pure string) so the prompt text is
    /// inspectable independent of the phone gate.
    static let systemPrompt = """
        You are a podcast advertising detector. You are given a 30-second transcript \
        window from a podcast. Decide whether the window is advertising content \
        (a paid ad read, sponsor spot, promo code, or programmatic ad insertion) or \
        regular show content. Reply with exactly one word: 'ad' or 'not_ad'.
        """

    /// The fine-tune user message, verbatim from `App.swift:25-27` /
    /// `prep_data.py`. Always compiled (pure string).
    static func userMessage(for text: String) -> String {
        "Transcript window:\n\"\"\"\n\(text)\n\"\"\"\n\nIs this window advertising? Answer 'ad' or 'not_ad'."
    }
}
