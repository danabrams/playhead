// SpecialistLiveRuntimeTests.swift
// playhead-b6jq PR 3 (Phase B2): FastTests (simulator) coverage for the
// phone-gated specialist engine + live-runtime factory.
//
// On simulator the engine path is compiled OUT, so these tests cover:
//   - the pure first-token-softmax math (device-agnostic, synthetic logits),
//   - the always-compiled bundled-model-URL resolver, and
//   - the phone gate itself (factory returns nil / provider throws
//     `.runtimeUnavailable` on simulator — mirrors the PR 1 availability probe).
//
// The live inference is exercised on a real device by a separate on-device
// smoke; it cannot run under FastTests.

import Foundation
import Testing
@testable import Playhead

/// Anchor for `Bundle(for:)` so a test can resolve the TEST bundle (which does
/// NOT carry the model folder reference) to exercise the "absent" path.
private final class SpecialistLiveRuntimeTestBundleToken {}

@Suite("Specialist first-token softmax (pure)")
struct SpecialistFirstTokenSoftmaxTests {

    @Test func adDominantLogitsYieldHighProbability() {
        let p = SpecialistFirstTokenSoftmax.probabilityOfAd(adLogit: 10, notLogit: 0)
        #expect(p > 0.99)
    }

    @Test func notDominantLogitsYieldLowProbability() {
        let p = SpecialistFirstTokenSoftmax.probabilityOfAd(adLogit: 0, notLogit: 10)
        #expect(p < 0.01)
    }

    @Test func equalLogitsYieldOneHalf() {
        let p = SpecialistFirstTokenSoftmax.probabilityOfAd(adLogit: 4.2, notLogit: 4.2)
        #expect(abs(p - 0.5) < 1e-9)
    }

    /// The posterior is the TWO-TOKEN normalization: `sigmoid(ad - not)`. A
    /// known gap of `ln 4` must give exactly `0.8`.
    @Test func knownGapMatchesClosedForm() {
        // ln(4) so exp(gap) = 4 -> P = 4/5 = 0.8.
        let p = SpecialistFirstTokenSoftmax.probabilityOfAd(adLogit: log(4.0), notLogit: 0)
        #expect(abs(p - 0.8) < 1e-9)
    }

    /// Swapping the two logits must reflect the probability about 0.5 (the
    /// two-token posterior is symmetric).
    @Test func swappingLogitsReflectsAboutHalf() {
        let forward = SpecialistFirstTokenSoftmax.probabilityOfAd(adLogit: 3, notLogit: -1)
        let reversed = SpecialistFirstTokenSoftmax.probabilityOfAd(adLogit: -1, notLogit: 3)
        #expect(abs((forward + reversed) - 1.0) < 1e-9)
    }

    /// Only the two LABEL logits matter: a huge logit elsewhere in the vocab
    /// (which would dominate a full-vocab argmax) must NOT change `P(ad)` —
    /// the full-softmax denominator cancels in the two-token ratio.
    @Test func fullVocabReadoutIgnoresOtherIndices() {
        var logits = [Double](repeating: 0, count: 2000)
        logits[SpecialistFirstTokenSoftmax.adTokenID] = log(4.0)  // gap -> P = 0.8
        logits[SpecialistFirstTokenSoftmax.notTokenID] = 0
        logits[500] = 100  // a dominant non-label logit that MUST be ignored
        let p = SpecialistFirstTokenSoftmax.probabilityOfAd(logits: logits)
        #expect(p != nil)
        #expect(abs((p ?? -1) - 0.8) < 1e-9)
    }

    @Test func outOfRangeIndexReturnsNil() {
        // Default label ids (329/1921) fall outside a 10-element vector.
        let p = SpecialistFirstTokenSoftmax.probabilityOfAd(logits: [Double](repeating: 0, count: 10))
        #expect(p == nil)
    }

    @Test func nonFiniteLabelLogitReturnsNil() {
        var logits = [Double](repeating: 0, count: 2000)
        logits[SpecialistFirstTokenSoftmax.adTokenID] = .nan
        let p = SpecialistFirstTokenSoftmax.probabilityOfAd(logits: logits)
        #expect(p == nil)
    }

    @Test func labelTokenIdsMatchFineTune() {
        // Pinned against the fine-tune tokenizer (coreai-spike widthspike).
        #expect(SpecialistFirstTokenSoftmax.adTokenID == 329)
        #expect(SpecialistFirstTokenSoftmax.notTokenID == 1921)
    }
}

@Suite("Specialist bundled-model resolution")
struct SpecialistModelResourcesTests {

    @Test func resolvesBundledModelWhenStaged() {
        // FastTests run hosted in Playhead.app; the model ships as a folder
        // reference (project.yml) and the pre-build guard fails the build if it
        // is not staged, so it is present in Bundle.main here.
        let url = SpecialistModelResources.bundledModelURL(in: .main)
        #expect(url != nil)
        if let url {
            #expect(url.lastPathComponent == SpecialistModelResources.modelFolderName)
            #expect(
                FileManager.default.fileExists(
                    atPath: url.appending(path: "metadata.json").path
                )
            )
        }
    }

    @Test func returnsNilWhenModelAbsentFromBundle() {
        // The TEST bundle carries no model folder reference (it is a resource of
        // the app target only), so the resolver must report absent.
        let testBundle = Bundle(for: SpecialistLiveRuntimeTestBundleToken.self)
        #expect(SpecialistModelResources.bundledModelURL(in: testBundle) == nil)
    }
}

@Suite("Specialist live-runtime phone gate")
struct SpecialistLiveRuntimePhoneGateTests {

    /// Mirrors the PR 1 availability probe's contract per destination: on
    /// simulator the engine is compiled out, so the live-runtime factory MUST
    /// return nil and the provider MUST refuse to classify.
    @Test func factoryIsGatedOutOnSimulator() {
        #if targetEnvironment(simulator)
        let runtime = SpecialistAdClassifier.makeLiveRuntime(
            modelURL: URL(fileURLWithPath: "/dev/null")
        )
        #expect(runtime == nil)
        #endif
    }

    @Test func providerRefusesToClassifyOnSimulator() async {
        #if targetEnvironment(simulator)
        let provider = SpecialistEngineProvider(modelURL: URL(fileURLWithPath: "/dev/null"))
        await #expect(throws: SpecialistEngineError.runtimeUnavailable) {
            _ = try await provider.classify(prompt: "brought to you by Acme, promo code SLEEP")
        }
        // Release is a safe no-op when nothing loaded.
        await provider.release()
        #endif
    }
}
