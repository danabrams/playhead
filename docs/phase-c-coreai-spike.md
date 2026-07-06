# Phase C spike — on-device fine-tuning for the ad classifier (playhead-xx7m.3)

**Status:** spike / investigation only — no code wired in (per no-unilateral-swaps).
**Date:** 2026-07-04. **SDKs read:** iOS 27.0 (Xcode 27 beta) and iOS 26.5 (Xcode 26.6), from the framework `.swiftinterface` files.

## TL;DR

The premise — "adopt iOS 27's **Core AI** framework to fine-tune the ad classifier on-device" — does **not** hold as stated:

1. **`CoreAI.framework` is not a developer API.** Its module ships an **empty** public Swift interface (0 public types/functions) and no headers — it is a private/system framework. The WWDC "Core AI" messaging does not correspond to a usable public framework.
2. **The real on-device customization surfaces are `FoundationModels` Adapters and `CreateML` — and both already existed in iOS 26.5.** Requiring iOS 27 grants **no new fine-tuning API**. (iOS 27's on-device gains for us remain the bigger model + ~32k context, i.e. Phase B.)

So Phase C should be reframed: not "adopt a new iOS 27 API," but "should we use the (already-available) on-device customization surfaces for the ad classifier?" — a decision independent of the iOS 27 floor.

## The two real surfaces

### A. FoundationModels `SystemLanguageModel.Adapter` — specialize the LLM (load-only on-device)
- `SystemLanguageModel(adapter:guardrails:)` loads a custom LoRA-style adapter onto the base on-device model.
- Adapters are distributed as **BackgroundAssets `AssetPack`s** (`Adapter.isCompatible(_: AssetPack)`, `compatibleAdapterIdentifiers(name:)`, `removeObsoleteAdapters()`). The `Adapter` struct exposes **no `train`/`fit`** — it is **load-only** on device.
- **Implication:** the adapter is **trained offline** (Apple's adapter-training toolkit on macOS/Python) on a curated dataset, then shipped/downloaded as an asset. Present since iOS 26.5.
- **Mandate ([[project_legal_ondevice]]):** compliant **iff** the adapter is trained on curated/synthetic ad data — no user data leaves the device (the adapter is a static asset). It does **not** learn from this user's corrections.

### B. `CreateML.MLTextClassifier` — train a lightweight classifier on-device
- Full CreateML training API is in the iOS SDK (since 26.5): `MLTextClassifier(trainingData:parameters:)` (annotated iOS 15.0+), `MLTrainingSessionParameters`, boosted-tree / logistic / random-forest classifiers, `MLClassifierMetrics`, etc.
- **Implication:** we could train a small text classifier **on-device** from the user's own labels in `FoundationModelsFeedbackStore`, entirely locally.
- **Mandate:** fully compliant — training and data stay on-device. This is the only path that gives **personalized, on-device learning** from user feedback.
- It is a **separate classic classifier**, not the LLM — it would act as an additional ad-detection signal/gate feeding the existing fusion, not a replacement for the FM classifier.

### C. `CoreAI.framework` — ruled out (no public API).

## Recommendation

If we pursue on-device learning at all, **Path B (CreateML `MLTextClassifier` from `FoundationModelsFeedbackStore`)** is the mandate-aligned option and the closest thing to genuine "on-device fine-tuning." Path A specializes the LLM but needs an offline training pipeline and doesn't personalize.

**But note:** neither path required the iOS 27 upgrade — both were available on iOS 26. So this is a standalone product decision, not part of "make use of iOS 27."

## Open question that gates Path B (needs a device)

CreateML's training initializers are *annotated* iOS 15+, but on-device CreateML **training** has historically been macOS-only at runtime despite the annotations. Whether `MLTextClassifier(trainingData:)` actually **trains on an iOS device** (vs throwing / being unsupported) must be confirmed with a **one-off device probe** — train a tiny text classifier on-device and check it returns a usable model + metrics. This is the single go/no-go for Path B and cannot be validated on the simulator.

## Suggested next step (if the user wants to proceed)

A minimal, throwaway device probe: an `MLTextClassifier` trained on ~a few dozen labeled ad/not-ad snippets, on-device, logging success + `trainingMetrics`. ~1 hour. If it trains on-device → design a real `OnDeviceAdClassifierTrainer` fed by `FoundationModelsFeedbackStore`; if it doesn't → Path B is off the table and only the offline-adapter Path A remains.
