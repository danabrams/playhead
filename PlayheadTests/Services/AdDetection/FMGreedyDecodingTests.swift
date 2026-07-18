// FMGreedyDecodingTests.swift
// playhead-xsdz.60: the `fmGreedyDecoding` flag pins the Foundation Model's
// live anchoring decoding to greedy (deterministic argmax) so the FM's
// `evidenceAnchors` list is reproducible run-to-run — fixing the auto-skip
// eligible-gate coin-flip (8↔49 atoms across runs). Default ON (greedy in
// production, approved 2026-07); reversible, and `false` is byte-identical to
// the pre-xsdz.60 call sites (`samplingMode` left nil).
//
// These are simulator-runnable (PlayheadFastTests) hermetic tests — they never
// boot the live FM stack. They pin the three seams the flag flows through:
//   1. `Config.fmGreedyDecoding` default (true) + reversibility + copy helper.
//   2. `FoundationModelClassifier.liveGenerationOptions(greedy:...)` — the ONE
//      option-builder the three `LiveSessionActor` respond paths call. Proven
//      via `GenerationOptions`/`SamplingMode` `Equatable` conformance: greedy
//      false == today's `GenerationOptions(maximumResponseTokens:)` (byte-
//      identical), greedy true carries `.greedy`, and the two differ.
//   3. `PipelineDumpLiveTests.fmGreedyDecodingOverrideFromEnvironment` — the
//      treatment lane's `PLAYHEAD_FM_GREEDY` runtime override resolver.

import Foundation
import Testing
@testable import Playhead

#if canImport(FoundationModels)
import FoundationModels
#endif

@Suite("playhead-xsdz.60 — FM greedy-decoding flag")
struct FMGreedyDecodingTests {

    // MARK: - Config: default, reversibility, copy helper

    @Test("Config.default pins fmGreedyDecoding ON (greedy in production)")
    func defaultIsGreedy() {
        #expect(FoundationModelClassifier.Config.default.fmGreedyDecoding == true)
    }

    @Test("the (safetyMarginTokens:maximumResponseTokens:) convenience init inherits the ON default")
    func convenienceInitDefaultsGreedy() {
        let config = FoundationModelClassifier.Config(
            safetyMarginTokens: 5,
            maximumResponseTokens: 6
        )
        #expect(config.fmGreedyDecoding == true)
    }

    @Test("fmGreedyDecoding is reversible: an explicit false is stored verbatim")
    func explicitFalseIsStored() {
        let config = FoundationModelClassifier.Config(
            safetyMarginTokens: 5,
            coarseMaximumResponseTokens: 6,
            refinementMaximumResponseTokens: 12,
            fmGreedyDecoding: false
        )
        #expect(config.fmGreedyDecoding == false)
    }

    @Test("withFMGreedyDecoding flips ONLY the flag; every other field survives")
    func copyHelperFlipsOnlyTheFlag() {
        let base = FoundationModelClassifier.Config.default
        let off = base.withFMGreedyDecoding(false)

        // The one intended difference.
        #expect(base.fmGreedyDecoding == true)
        #expect(off.fmGreedyDecoding == false)

        // Everything else is carried through unchanged (Config is not
        // Equatable, so compare field-by-field).
        #expect(off.safetyMarginTokens == base.safetyMarginTokens)
        #expect(off.coarseMaximumResponseTokens == base.coarseMaximumResponseTokens)
        #expect(off.refinementMaximumResponseTokens == base.refinementMaximumResponseTokens)
        #expect(off.zoomAmbiguityBudget == base.zoomAmbiguityBudget)
        #expect(off.minimumZoomSpanLines == base.minimumZoomSpanLines)
        #expect(off.maximumRefinementSpansPerWindow == base.maximumRefinementSpansPerWindow)

        // Round-trip back to true is a no-op on every field.
        let backOn = off.withFMGreedyDecoding(true)
        #expect(backOn.fmGreedyDecoding == true)
    }

    // MARK: - Treatment-lane env override resolver

    @Test("PLAYHEAD_FM_GREEDY resolver: 1 → greedy, 0 → stochastic, absent/garbage → default")
    func envOverrideResolver() {
        #expect(PipelineDumpLiveTests.fmGreedyDecodingOverrideFromEnvironment(
            ["PLAYHEAD_FM_GREEDY": "1"]) == true)
        #expect(PipelineDumpLiveTests.fmGreedyDecodingOverrideFromEnvironment(
            ["PLAYHEAD_FM_GREEDY": "0"]) == false)
        // Absent → nil → "use the Config default".
        #expect(PipelineDumpLiveTests.fmGreedyDecodingOverrideFromEnvironment([:]) == nil)
        // Any other value is not an override.
        #expect(PipelineDumpLiveTests.fmGreedyDecodingOverrideFromEnvironment(
            ["PLAYHEAD_FM_GREEDY": "true"]) == nil)
        #expect(PipelineDumpLiveTests.fmGreedyDecodingOverrideFromEnvironment(
            ["PLAYHEAD_FM_GREEDY": ""]) == nil)
    }

    // MARK: - The option-builder seam (the three respond paths route through it)

    #if canImport(FoundationModels)
    @available(iOS 26.0, *)
    @Test("greedy=false is byte-identical to today; greedy=true carries .greedy")
    func optionBuilderSamplingMode() {
        let tokens = 96

        // greedy == false ⇒ samplingMode nil ⇒ EQUATABLE-equal to the exact
        // pre-xsdz.60 call `GenerationOptions(maximumResponseTokens:)`.
        let today = GenerationOptions(maximumResponseTokens: tokens)
        let off = FoundationModelClassifier.liveGenerationOptions(
            greedy: false, maximumResponseTokens: tokens)
        #expect(off == today)
        #expect(off.samplingMode == nil)

        // greedy == true ⇒ samplingMode .greedy.
        let on = FoundationModelClassifier.liveGenerationOptions(
            greedy: true, maximumResponseTokens: tokens)
        #expect(on == GenerationOptions(samplingMode: .greedy, maximumResponseTokens: tokens))
        #expect(on.samplingMode == .greedy)

        // The flag actually changes the emitted options (not a tautology).
        #expect(on != off)

        // The token budget is preserved on both branches.
        #expect(off.maximumResponseTokens == tokens)
        #expect(on.maximumResponseTokens == tokens)
    }
    #endif
}
