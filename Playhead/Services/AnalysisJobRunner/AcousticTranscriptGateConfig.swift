// AcousticTranscriptGateConfig.swift
// playhead-gtt9.1: shadow-mode configuration for the acoustic-likelihood-driven
// transcript scheduler.
//
// Most transcript work today is spent on regions whose acoustic features
// (no music bed, no speaker change, no spectral flux) suggest the audio is
// pure host conversation — i.e. nothing the ad detector wants. The 2026-04-26
// dogfood capture showed 105,839 decision-log entries against just 147 fusion-
// scored windows (0.14%), the bottleneck being `BackfillEvidenceFusion
// .quorumGateForFMConsensus` which requires `distinctKinds.count >= 2`. Because
// transcript dominates the CPU/battery cost in the analysis pipeline, gating
// transcript scheduling on acoustic likelihood is the highest-leverage
// optimization we have left.
//
// **This file ships the gate as shadow-only.** With the defaults below the
// runner *computes* the gate decision and *logs* the would-skip outcome but
// continues to transcribe every shard. We collect ≥1 dogfood capture's worth
// of shadow telemetry and measure host-read miss rate before flipping
// `skipEnabled` true in a follow-up bead.
//
// The struct shape mirrors `MetadataActivationConfig`: a master `enabled` kill
// gate plus per-stage flags that take effect only when the master is open. The
// `is*Active` computed properties are the AND of `(enabled && per-stage)` so
// every consumer reads through one consistent surface.

import Foundation

// MARK: - AcousticTranscriptGateConfig

/// Gates and thresholds for the acoustic-likelihood transcript scheduler.
///
/// `enabled` is the master kill: when false, the runner never invokes
/// `AcousticLikelihoodScorer.scoreOne` and never emits a shadow-gate log entry
/// — production behavior is byte-identical to the pre-gtt9.1 runner.
///
/// When `enabled = true`, the runner scores each shard and emits a structured
/// shadow-log row carrying the decision (`would-skip` | `safety-sample-keep` |
/// `quality-precondition-keep`). Whether the runner *actually* skips
/// transcription on `would-skip` shards is governed by the independent
/// `skipEnabled` flag — the default ships with `enabled = true,
/// skipEnabled = false` (shadow logging on, production skip off).
struct AcousticTranscriptGateConfig: Sendable, Equatable {

    /// Master kill — when false, the gate is fully inert: no scoring, no
    /// logging, no shadow rows written. Production behavior matches the
    /// pre-gtt9.1 runner exactly.
    let enabled: Bool

    /// Whether `wouldGate=true` shards are actually withheld from
    /// `TranscriptEngineService.startTranscription`. When false, the gate
    /// computes and logs but every shard still reaches the engine — this is
    /// the shadow-mode shape we ship as the default.
    let skipEnabled: Bool

    /// Minimum acoustic likelihood (`AcousticLikelihoodScorer.scoreOne` output,
    /// range `[0, 1]`) to keep transcribing. Shards whose scored likelihood is
    /// strictly less than this threshold are tagged `wouldGate=true`.
    ///
    /// Default 0.30 is calibrated against the scorer's bounded-additive
    /// combiner: a window with one moderate-strength acoustic cue (e.g.
    /// background music bed at level 0.5 weighted 0.10, plus modest speaker-
    /// change at 0.3 weighted 0.20) lands around 0.11, while a clear ad onset
    /// (foreground music bed + onset score + speaker change) lands well above
    /// 0.5. The 0.30 cutoff sits between those regimes; we'll tune it from
    /// shadow-eval data in a follow-up.
    let likelihoodThreshold: Double

    /// Fraction of `wouldGate=true` shards to keep transcribing as a calibration
    /// safety sample. `0.10` = 10% of would-skip shards bypass the gate so we
    /// continuously measure host-read miss rate even after `skipEnabled` flips.
    /// Set to `0.0` to disable the safety sample entirely.
    let safetySampleFraction: Double

    // MARK: - Defaults

    /// Production default: shadow logging ON (`enabled = true`) but production
    /// skip OFF (`skipEnabled = false`). The runner scores every shard,
    /// writes a shadow-log row carrying the decision, and continues to
    /// transcribe every shard regardless.
    ///
    /// `safetySampleFraction = 0.10` exercises the would-skip → safety-sample-
    /// keep code path on ~10% of would-skip shards even in shadow mode, so
    /// the future `skipEnabled = true` flip ships against a safety-sample
    /// implementation that's already battle-tested by dogfood.
    static let `default` = AcousticTranscriptGateConfig(
        enabled: true,
        skipEnabled: false,
        likelihoodThreshold: 0.30,
        safetySampleFraction: 0.10
    )

    /// Fully off — used by tests asserting that the disabled-master path is
    /// inert (no scoring calls, no shadow log rows, every shard transcribed).
    static let disabled = AcousticTranscriptGateConfig(
        enabled: false,
        skipEnabled: false,
        likelihoodThreshold: 0.30,
        safetySampleFraction: 0.0
    )

    // MARK: - Effective state

    /// Whether the shadow-mode evaluation path runs (scoring + logging).
    /// Mirrors the `is*Active` shape used by `MetadataActivationConfig` so
    /// consumers all read through the same single boolean per stage.
    var isShadowLoggingActive: Bool {
        enabled
    }

    /// Whether `wouldGate=true` shards are actually withheld from the
    /// transcript engine. AND of master + per-stage flag — this is the gate
    /// the runner consults at filter time.
    var isProductionSkipActive: Bool {
        enabled && skipEnabled
    }
}
