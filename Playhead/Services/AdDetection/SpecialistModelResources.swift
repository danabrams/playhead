// SpecialistModelResources.swift
// playhead-b6jq PR 3 (Phase B2): always-compiled, device-agnostic helpers
// shared by the phone-gated specialist engine.
//
// Everything in this file compiles on EVERY destination (no CoreAI import):
//   - `SpecialistModelResources` resolves the bundled model directory.
//   - `SpecialistFirstTokenSoftmax` is the pure "first-token logits -> P(ad)"
//     math, factored out so the simulator FastTests can cover it with
//     synthetic logit vectors (the live engine that FEEDS it real logits is
//     phone-gated and cannot run under FastTests).
//
// No state, no I/O beyond a single existence probe, no model. See
// `SpecialistEngineProvider` for the phone-gated engine that calls into this.

import Foundation

// MARK: - Bundled model resolution

/// Resolves the on-device specialist model directory bundled into the app.
///
/// The model ships as a *folder reference* (`project.yml` `type: folder`) so its
/// nested `.aimodel/` + `tokenizer/` tree is preserved verbatim and loadable at
/// `Bundle.main.resourceURL/qwen3_0_6b_4bit_dynamic_ft_v2/` — exactly the path
/// the phaseB spike loader used (`App.swift:125-128`).
///
/// Always compiled: this is a plain path + existence check with no engine
/// dependency, so callers (and PR 4's injection site) do not need to be
/// phone-gated to *ask* whether the model is present.
enum SpecialistModelResources {
    /// Folder-reference name bundled by `project.yml`. Matches the export dir
    /// name and the phaseB spike (`qwen3_0_6b_4bit_dynamic_ft_v2`).
    static let modelFolderName = "qwen3_0_6b_4bit_dynamic_ft_v2"

    /// URL of the bundled specialist model directory, or `nil` when it is not
    /// staged into the given bundle.
    ///
    /// Presence is probed via the model's `metadata.json` (the cheapest
    /// always-present marker; mirrors the phaseB spike's existence check at
    /// `App.swift:127-128`). Returning `nil` — rather than a URL that may not
    /// exist — lets the caller distinguish "model bundled" from "model absent"
    /// without a second filesystem hit.
    ///
    /// - Parameter bundle: Bundle to resolve within. Defaults to `.main` (the
    ///   host app for a hosted unit-test run); tests pass a bundle that lacks
    ///   the model to exercise the absent path.
    static func bundledModelURL(in bundle: Bundle = .main) -> URL? {
        guard let resourceURL = bundle.resourceURL else { return nil }
        let dir = resourceURL.appending(path: modelFolderName)
        let metadata = dir.appending(path: "metadata.json")
        guard FileManager.default.fileExists(atPath: metadata.path) else { return nil }
        return dir
    }
}

// MARK: - First-token softmax (pure)

/// Pure "first-token logits -> P(ad)" math for the distilled specialist.
///
/// The fine-tuned model answers `'ad'` or `'not_ad'` as its FIRST decoded
/// token. We never free-generate the word (the brittle
/// `ConstrainedGenerationSession` / object-JSON path is deliberately avoided):
/// one forward pass yields the next-token logits, and the calibrated verdict is
/// the two-token normalized posterior over the `'ad'` (`329`) and `'not'`
/// (`1921`) token ids.
///
/// This reduces the reference width-spike computation
/// (`softmax(logits)[ad] / (softmax(logits)[ad] + softmax(logits)[not])`, see
/// `coreai-spike/widthspike/full_episode_infer_v2.py`) to its numerically
/// stable, mathematically identical form: the full-vocab softmax denominator
/// `Z` cancels in the ratio, leaving `sigmoid(logit_ad - logit_not)`.
enum SpecialistFirstTokenSoftmax {
    /// First-token id for `'ad'` in the fine-tuned tokenizer.
    /// (`coreai-spike/widthspike/*.py`: `AD_ID = 329`.)
    static let adTokenID = 329
    /// First-token id for `'not'` (leading token of `'not_ad'`).
    /// (`coreai-spike/widthspike/*.py`: `NOT_ID = 1921`.)
    static let notTokenID = 1921

    /// Two-token normalized posterior `P(ad)` from the two label logits.
    ///
    /// Equivalent to `pad / (pad + pnot)` after a full-vocab softmax (the `Z`
    /// cancels), computed here as the stable sigmoid of the logit gap so a huge
    /// dominant logit elsewhere in the vocab cannot overflow.
    ///
    /// - Returns: A probability in `0...1`. `0.5` when the two logits are equal.
    static func probabilityOfAd(adLogit: Double, notLogit: Double) -> Double {
        let gap = adLogit - notLogit
        // Branch on sign for numerical stability (avoid exp of a large +value).
        if gap >= 0 {
            return 1.0 / (1.0 + exp(-gap))
        } else {
            let e = exp(gap)
            return e / (1.0 + e)
        }
    }

    /// Two-token normalized posterior `P(ad)` read out of a full next-token
    /// logits vector at the `'ad'` / `'not'` indices.
    ///
    /// - Returns: `P(ad)` in `0...1`, or `nil` when either index is out of
    ///   range for `logits`, or either selected logit is non-finite (a rogue
    ///   model output that must not poison a downstream threshold).
    static func probabilityOfAd(
        logits: [Double],
        adTokenID: Int = adTokenID,
        notTokenID: Int = notTokenID
    ) -> Double? {
        guard adTokenID >= 0, notTokenID >= 0,
              adTokenID < logits.count, notTokenID < logits.count
        else { return nil }
        let adLogit = logits[adTokenID]
        let notLogit = logits[notTokenID]
        guard adLogit.isFinite, notLogit.isFinite else { return nil }
        return probabilityOfAd(adLogit: adLogit, notLogit: notLogit)
    }
}
