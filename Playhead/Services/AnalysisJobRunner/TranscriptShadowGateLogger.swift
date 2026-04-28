// TranscriptShadowGateLogger.swift
// playhead-gtt9.1: per-shard structured-event sink for the shadow-mode
// acoustic-likelihood transcript gate.
//
// `AnalysisJobRunner` emits one `TranscriptShadowGateEntry` per shard it
// considers for transcription. Entries carry the per-shard likelihood, the
// gate threshold, and the categorical decision (would-skip /
// safety-sample-keep / quality-precondition-keep / above-threshold /
// score-unknown) plus a `transcribed: Bool` recording whether the shard
// actually reached the transcript engine. Replay tooling reads these rows
// to compute would-have-skipped recall against host-read ground truth
// before the team flips `AcousticTranscriptGateConfig.skipEnabled` to true.
//
// Why a separate sink (vs. piggy-backing on `DecisionLogger`)? `DecisionLogger`'s
// schema is rigid — every row is a per-window classifier decision with a fused-
// confidence breakdown. Stuffing shard-level scheduling decisions into that
// schema would either bloat every existing window record with empty optionals
// or break replay tooling that joins on `windowBounds`. A dedicated narrow
// schema lets shadow-eval consumers parse the JSONL stream without the
// noise from the much larger window-decision corpus.

import Foundation
import OSLog

// MARK: - TranscriptShadowGateEntry (JSONL record schema)

/// Schema-versioned, Codable record for one shadow-gate decision.
///
/// One row is emitted per shard the runner considers in
/// `evaluateAcousticTranscriptGate`. Fields mirror the inputs to the
/// gate decision so the eval pipeline can reproduce it offline:
///   * `likelihood` is the `AcousticLikelihoodScorer.maxLikelihoodInSpan`
///     output for the shard's `[startTime, endTime)` span. `nil` when no
///     persisted `feature_windows` row overlapped the shard.
///   * `threshold` is the active `likelihoodThreshold` at decision time.
///   * `decision` records the categorical outcome.
///   * `wouldGate` is `true` iff the shard would be skipped under
///     production-skip mode (i.e. `decision == .wouldSkip`). The
///     `safety-sample-keep` arm reports `wouldGate=true` with
///     `transcribed=true` so eval can distinguish "kept by sampling"
///     from "kept because likelihood passed the threshold".
///   * `transcribed` is `true` iff the shard was actually handed to the
///     transcript engine. In shadow mode this is `true` for every row
///     except the production-skip + would-skip case (which never fires
///     under default config).
struct TranscriptShadowGateEntry: Codable, Equatable, Sendable {

    /// Schema version; increment on breaking changes. Current: 2.
    let schemaVersion: Int

    /// Unix time at which the decision was emitted (seconds since epoch).
    let timestamp: Double

    /// Analysis-asset content fingerprint. Joins to `AnalysisAsset.id`.
    let analysisAssetID: String

    /// Episode identifier carried by the originating
    /// `AnalysisRangeRequest`. Useful for cross-episode rollups during
    /// shadow-eval (the asset id is per-fingerprint, not per-episode).
    let episodeID: String

    /// Shard identifier — `AnalysisShard.id` is unique within an episode.
    let shardID: Int

    /// Shard span start in episode-relative seconds.
    let shardStart: Double

    /// Shard span end in episode-relative seconds (exclusive).
    let shardEnd: Double

    /// Acoustic likelihood produced by `maxLikelihoodInSpan`. `nil` when
    /// no overlapping `feature_windows` row was persisted at the moment
    /// the gate ran — typically a fresh feature-extraction race or a
    /// `feature_version` skew. Eval treats `nil` as "score unknown" and
    /// never counts that shard in the would-skip rate.
    let likelihood: Double?

    /// Active `likelihoodThreshold` at decision time. Captured per-row so
    /// eval can detect threshold drift in the historical corpus.
    let threshold: Double

    /// Categorical outcome — see the `Decision` enum.
    let decision: Decision

    /// True iff the shard would be withheld from the transcript engine
    /// under production-skip mode. The `safety-sample-keep` arm reports
    /// `wouldGate=true, transcribed=true` so consumers can distinguish
    /// "kept by sampling" from "kept because likelihood passed".
    let wouldGate: Bool

    /// True iff the shard was actually handed to the transcript engine.
    /// Shadow-mode rows carry `transcribed=true` for every category;
    /// production-skip rows carry `transcribed=false` only for `.wouldSkip`.
    let transcribed: Bool

    /// Short git SHA stamped at logger init from `BuildInfo.commitSHA`.
    /// Always set on v2 rows (falls back to `"unknown"` outside a git
    /// context per the `BuildInfo` contract). Decodes as `nil` on v1
    /// rows so pre-bump captures round-trip cleanly.
    let buildCommitSHA: String?

    static let currentSchemaVersion: Int = 2

    // playhead-b58j: explicit Codable so v1 rows (no buildCommitSHA key)
    // decode cleanly with `buildCommitSHA = nil`. v2 always emits the
    // key (even when nil → JSON null) so consumers self-identify the
    // capture cohort. Mirrors the DecisionLogEntry.LedgerEntry pattern
    // from playhead-epfk.
    private enum CodingKeys: String, CodingKey {
        case schemaVersion, timestamp, analysisAssetID, episodeID,
             shardID, shardStart, shardEnd, likelihood, threshold,
             decision, wouldGate, transcribed, buildCommitSHA
    }

    init(
        schemaVersion: Int,
        timestamp: Double,
        analysisAssetID: String,
        episodeID: String,
        shardID: Int,
        shardStart: Double,
        shardEnd: Double,
        likelihood: Double?,
        threshold: Double,
        decision: Decision,
        wouldGate: Bool,
        transcribed: Bool,
        buildCommitSHA: String?
    ) {
        self.schemaVersion = schemaVersion
        self.timestamp = timestamp
        self.analysisAssetID = analysisAssetID
        self.episodeID = episodeID
        self.shardID = shardID
        self.shardStart = shardStart
        self.shardEnd = shardEnd
        self.likelihood = likelihood
        self.threshold = threshold
        self.decision = decision
        self.wouldGate = wouldGate
        self.transcribed = transcribed
        self.buildCommitSHA = buildCommitSHA
    }

    init(from decoder: Decoder) throws {
        let c = try decoder.container(keyedBy: CodingKeys.self)
        self.schemaVersion = try c.decode(Int.self, forKey: .schemaVersion)
        self.timestamp = try c.decode(Double.self, forKey: .timestamp)
        self.analysisAssetID = try c.decode(String.self, forKey: .analysisAssetID)
        self.episodeID = try c.decode(String.self, forKey: .episodeID)
        self.shardID = try c.decode(Int.self, forKey: .shardID)
        self.shardStart = try c.decode(Double.self, forKey: .shardStart)
        self.shardEnd = try c.decode(Double.self, forKey: .shardEnd)
        self.likelihood = try c.decodeIfPresent(Double.self, forKey: .likelihood)
        self.threshold = try c.decode(Double.self, forKey: .threshold)
        self.decision = try c.decode(Decision.self, forKey: .decision)
        self.wouldGate = try c.decode(Bool.self, forKey: .wouldGate)
        self.transcribed = try c.decode(Bool.self, forKey: .transcribed)
        // playhead-b58j: pre-bump (v1) rows omit the key → nil.
        self.buildCommitSHA = try c.decodeIfPresent(String.self, forKey: .buildCommitSHA)
    }

    func encode(to encoder: Encoder) throws {
        var c = encoder.container(keyedBy: CodingKeys.self)
        try c.encode(schemaVersion, forKey: .schemaVersion)
        try c.encode(timestamp, forKey: .timestamp)
        try c.encode(analysisAssetID, forKey: .analysisAssetID)
        try c.encode(episodeID, forKey: .episodeID)
        try c.encode(shardID, forKey: .shardID)
        try c.encode(shardStart, forKey: .shardStart)
        try c.encode(shardEnd, forKey: .shardEnd)
        try c.encode(likelihood, forKey: .likelihood)
        try c.encode(threshold, forKey: .threshold)
        try c.encode(decision, forKey: .decision)
        try c.encode(wouldGate, forKey: .wouldGate)
        try c.encode(transcribed, forKey: .transcribed)
        // Always emit (even when nil → JSON null) so v2 rows are wire-
        // distinguishable from v1.
        try c.encode(buildCommitSHA, forKey: .buildCommitSHA)
    }

    /// Categorical decision for a shadow-gate evaluation.
    enum Decision: String, Codable, Equatable, Sendable {
        /// Likelihood ≥ threshold. The acoustic prior says "transcribe."
        case aboveThreshold

        /// Likelihood < threshold but the safety-sample coin came up
        /// heads. The shard is transcribed anyway so we keep a calibration
        /// stream of low-likelihood ground truth even after `skipEnabled`
        /// flips.
        case safetySampleKeep

        /// Likelihood < threshold and the safety-sample coin came up
        /// tails. In shadow mode the shard is still transcribed; in
        /// production-skip mode it is dropped from the engine input.
        case wouldSkip

        /// Asset's persisted fast-transcript watermark already covers
        /// this shard — we're re-running over good transcript. M1
        /// mitigation: never gate out a shard whose region already has
        /// transcript chunks the rest of the pipeline depends on.
        case qualityPreconditionKeep

        /// No `feature_windows` row overlapped the shard at decision
        /// time. Defensive: never gate out unknowns.
        case scoreUnknown
    }
}

// MARK: - TranscriptShadowGateLogging

/// Protocol seam for the shadow-gate sink. The release build installs
/// `NoOpTranscriptShadowGateLogger`; DEBUG/dogfood builds install the
/// real JSONL writer (added in a follow-up wiring bead — production has
/// no consumer yet). Tests inject a recording stub.
protocol TranscriptShadowGateLogging: Sendable {
    /// Append a single shadow-gate record. Must not block the caller
    /// beyond the actor hop; file I/O is serialized inside the
    /// implementation when one is configured.
    func record(_ entry: TranscriptShadowGateEntry) async
}

/// Default logger used in production until the dogfood-write bead lands.
/// Every `record(_:)` call is dropped silently — the runner still pays
/// the ~hundred-byte allocation per shard, but no I/O happens.
struct NoOpTranscriptShadowGateLogger: TranscriptShadowGateLogging {
    func record(_ entry: TranscriptShadowGateEntry) async {
        // intentionally blank
    }
}
