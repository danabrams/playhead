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

    /// Map an `AdType` from this corpus into the `GroundTruthAdSegment.AdSegmentType`
    /// the existing simulator expects. The corpus is richer than the
    /// simulator's pre-existing enum, so we map best-effort:
    ///   - `host_read`, `blended_host_read`, `produced_segment`, `promo`
    ///     all become `midRoll` (positional) by default.
    ///   - `dynamic_insertion` stays as `dynamicInsertion` so
    ///     simulator-side variant tests can detect it.
    private func mappedAdType() -> GroundTruthAdSegment.AdSegmentType {
        // Position-aware mapping requires the window's location relative
        // to the episode timeline; until the upstream simulator gains a
        // dedicated category for this corpus we use `midRoll` as the
        // closest neutral value (the existing enum has no "unspecified").
        .midRoll
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
    /// `GroundTruthAdSegment` array.
    func groundTruthSegments() -> [GroundTruthAdSegment] {
        adWindows.map { w in
            GroundTruthAdSegment(
                startTime: w.startSeconds,
                endTime: w.endSeconds,
                advertiser: w.advertiser,
                product: w.product,
                adType: mappedAdType(),
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
