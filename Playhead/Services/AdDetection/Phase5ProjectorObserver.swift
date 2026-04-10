// Phase5ProjectorObserver.swift
// playhead-4my.5 (Phase 5):
//
// Observation-only sink for the Phase 5 AtomEvidenceProjector +
// MinimalContiguousSpanDecoder pipeline. Mirrors the contract of
// RegionShadowObserver (Phase 4 shadow wire-up).
//
// Contract:
//   • Compiled in all configurations. Step 11 only runs when an observer
//     is injected; production release builds never construct one.
//   • Writes are per-asset. Repeated writes for the same asset overwrite.
//   • Actor for safe cross-concurrency-domain access from tests.

import Foundation
import OSLog

actor Phase5ProjectorObserver {

    private let logger = Logger(
        subsystem: "com.playhead",
        category: "Phase5ProjectorObserver"
    )

    private var latestSpans: [String: [DecodedSpan]] = [:]
    private var latestEvidence: [String: [AtomEvidence]] = [:]
    private var recordCounts: [String: Int] = [:]

    init() {}

    /// Record Phase 5 output for an asset.
    func record(assetId: String, spans: [DecodedSpan], evidence: [AtomEvidence]) {
        latestSpans[assetId] = spans
        latestEvidence[assetId] = evidence
        recordCounts[assetId, default: 0] += 1
        logger.debug(
            "Recorded \(spans.count, privacy: .public) decoded spans, \(evidence.count, privacy: .public) evidence for asset \(assetId, privacy: .public)"
        )
    }

    /// Most recently recorded decoded spans for an asset, or nil if none recorded.
    func latestDecodedSpans(for assetId: String) -> [DecodedSpan]? {
        latestSpans[assetId]
    }

    /// Most recently recorded atom evidence for an asset, or nil if none recorded.
    func latestAtomEvidence(for assetId: String) -> [AtomEvidence]? {
        latestEvidence[assetId]
    }

    /// Number of times `record` has been called for an asset.
    func recordCount(for assetId: String) -> Int {
        recordCounts[assetId, default: 0]
    }
}
