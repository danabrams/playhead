// CorpusAnnotation+ReplayAdapter.swift
// Adapter that wires the playhead-l2f `CorpusAnnotation` into the
// existing `ReplaySimulator` harness without modifying the simulator's
// algorithm.
//
// The simulator reads `ReplayConfiguration`, which expects:
//   - `groundTruthSegments: [GroundTruthAdSegment]` for ad regions
//   - `transcriptChunks: [TranscriptChunk]` for the analysis pipeline
//   - `featureWindows: [FeatureWindow]` for boundary snapping
//
// Until real audio + real transcripts are paired with each annotation,
// this adapter generates synthetic transcript chunks (10s spans) that
// span the full episode duration so the simulator can drive its
// virtual playhead end to end. Real chunks/feature-windows will be
// wired in by the follow-up bead that processes the audio offline.

import Foundation
@testable import Playhead

extension CorpusAnnotation {

    // MARK: - Position-derived AdSegmentType thresholds
    //
    // The L2F corpus carries no explicit pre/mid/post-roll label — the
    // window's position on the episode timeline is the only signal we
    // have. We classify an ad window by where it sits relative to the
    // episode boundaries:
    //
    //   * `start_seconds < threshold`                       → .preRoll
    //   * `end_seconds   > duration_seconds - threshold`    → .postRoll
    //   * otherwise                                         → .midRoll
    //
    // The threshold uses an OR-style rule: the *larger* of an absolute
    // floor (`absoluteThresholdSeconds`) and a duration-relative ratio
    // (`relativeThresholdFraction`). This means an ad qualifies as
    // pre/post-roll if EITHER condition is satisfied — the more
    // permissive interpretation. Concretely:
    //
    //     effective_threshold = max(30 s, 0.01 * duration_seconds)
    //
    // Rationale for the constants:
    //   - 30 s absolute floor: typical podcast pre-rolls run 15-30 s
    //     (e.g. iHeart, Wondery, Spotify host-reads), so a 30 s ad
    //     positioned at t=0 is unambiguously pre-roll. Anything later
    //     is more likely a programmed mid-roll break.
    //   - 1 % duration-relative arm: a 60-minute show has a 36 s
    //     pre-roll budget, a 3-hour show has 108 s — proportional to
    //     the show's length so very long episodes don't accidentally
    //     classify a 45 s post-intro segment as mid-roll.
    //   - We take the LARGER of the two so a 30 s ad in a 60-minute
    //     episode (30 s < 36 s) classifies as .preRoll, matching
    //     intuition.
    //
    // Off-by-one: comparisons are STRICT (`<` and `>`). An ad
    // starting at exactly the threshold boundary is treated as
    // mid-roll, on the theory that the boundary case is ambiguous and
    // mid-roll is the safer default (the corpus's mid-roll counts are
    // larger and a misclassification there is less analytically
    // disruptive).
    //
    // Precedence when both arms match (extremely short episodes
    // where pre + post > duration, or an ad that spans the whole
    // episode): pre-roll wins. This is deterministic and keeps the
    // classification stable as duration shrinks past the threshold
    // sum; the alternative (post-roll wins) would cause a 30 s
    // episode-spanning ad to flip from pre-roll to post-roll once
    // the episode crosses 60 s, which is harder to reason about.

    /// Absolute threshold (in seconds) that classifies an ad as
    /// pre-roll if it starts before this point — or as post-roll if it
    /// ends after `duration - this`. See the file-level comment for
    /// the rationale behind 30 seconds.
    static let absoluteThresholdSeconds: Double = 30.0

    /// Duration-relative arm of the threshold rule. 1 % of the
    /// episode duration; combined with `absoluteThresholdSeconds` via
    /// `max(...)`.
    static let relativeThresholdFraction: Double = 0.01

    /// Effective pre/post-roll boundary threshold for this annotation,
    /// in seconds. The `max(absolute, relative)` form implements the
    /// "either condition" semantics described in the file header.
    /// Used symmetrically: an ad qualifies as pre-roll if
    /// `start_seconds` is below this value, or as post-roll if
    /// `end_seconds` exceeds `duration_seconds - this`.
    var rollBoundaryThresholdSeconds: Double {
        max(Self.absoluteThresholdSeconds, Self.relativeThresholdFraction * durationSeconds)
    }

    /// Map an ad window to the simulator's `AdSegmentType` based on
    /// where it sits on the episode timeline. See the file header for
    /// the threshold derivation, off-by-one decision, and tiny-episode
    /// precedence rule.
    ///
    /// Note: this intentionally ignores the corpus `AdType` enum
    /// (`hostRead`, `dynamicInsertion`, etc.) — that's an orthogonal
    /// axis (insertion style) which is captured separately via
    /// `DeliveryStyle`. `AdSegmentType` here describes timeline
    /// position only.
    private func mappedAdType(for window: AdWindow) -> GroundTruthAdSegment.AdSegmentType {
        let threshold = rollBoundaryThresholdSeconds
        let isPreRoll = window.startSeconds < threshold
        let isPostRoll = window.endSeconds > durationSeconds - threshold
        // Pre-roll wins when both arms match (e.g. tiny episodes where
        // the threshold sum exceeds duration, or an ad that spans the
        // full episode).
        if isPreRoll { return .preRoll }
        if isPostRoll { return .postRoll }
        return .midRoll
    }

    /// Map an `AdType` to the simulator's `DeliveryStyle`.
    private static func mapDeliveryStyle(
        _ type: CorpusAnnotation.AdType
    ) -> GroundTruthAdSegment.DeliveryStyle {
        switch type {
        case .hostRead: .hostRead
        case .dynamicInsertion: .dynamicInsertion
        case .blendedHostRead: .blendedHostRead
        case .producedSegment: .producedSegment
        case .promo: .producedSegment
        }
    }

    /// Convert this annotation's `ad_windows` into the simulator's
    /// `GroundTruthAdSegment` array. Each window's `AdSegmentType` is
    /// derived from its timeline position (see `mappedAdType(for:)`).
    func groundTruthSegments() -> [GroundTruthAdSegment] {
        adWindows.map { w in
            GroundTruthAdSegment(
                startTime: w.startSeconds,
                endTime: w.endSeconds,
                advertiser: w.advertiser,
                product: w.product,
                adType: mappedAdType(for: w),
                deliveryStyle: Self.mapDeliveryStyle(w.adType)
            )
        }
    }

    /// Build a minimal `ReplayConfiguration` so this annotation can be
    /// fed into the existing `ReplaySimulator` end to end.
    ///
    /// Synthetic transcript chunks (10s) span the full episode; no
    /// `featureWindows` or `dynamicAdVariants` are emitted yet — those
    /// will be populated when the audio is processed offline.
    /// `chunkDuration` of 10s matches the legacy `CorpusLoader.makeReplayConfig`
    /// helper; keeping the value aligned avoids accidental cross-corpus
    /// drift in metrics that bucket by chunk index.
    func makeReplayConfiguration(
        condition: SimulationCondition,
        timeStep: TimeInterval = ReplayConfiguration.defaultTimeStep,
        chunkDuration: TimeInterval = 10.0
    ) -> ReplayConfiguration {
        let chunks = stride(from: 0.0, to: durationSeconds, by: chunkDuration).map { start -> TranscriptChunk in
            let end = min(start + chunkDuration, durationSeconds)
            return TranscriptChunk(
                id: "l2f-\(episodeId)-\(Int(start))",
                analysisAssetId: episodeId,
                segmentFingerprint: "l2f-fp-\(Int(start))",
                chunkIndex: Int(start / chunkDuration),
                startTime: start,
                endTime: end,
                text: "Synthetic transcript chunk for L2F corpus replay.",
                normalizedText: "synthetic transcript chunk for l2f corpus replay",
                pass: "fast",
                modelVersion: "l2f-corpus-v1",
                transcriptVersion: nil,
                atomOrdinal: nil
            )
        }
        return ReplayConfiguration(
            episodeId: episodeId,
            episodeTitle: showName,
            podcastId: showName,
            episodeDuration: durationSeconds,
            condition: condition,
            groundTruthSegments: groundTruthSegments(),
            transcriptChunks: chunks,
            featureWindows: [],
            dynamicAdVariants: [],
            timeStep: timeStep
        )
    }
}
