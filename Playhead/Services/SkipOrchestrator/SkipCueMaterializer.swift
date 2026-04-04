// SkipCueMaterializer.swift
// Transforms AdWindows into SkipCues with dedup via cueHash.
// Plain struct (NOT an actor) doing stateless transforms + store writes.

import CryptoKit
import Foundation

struct SkipCueMaterializer: Sendable {
    let store: AnalysisStore
    let confidenceThreshold: Double

    init(store: AnalysisStore, confidenceThreshold: Double = 0.7) {
        self.store = store
        self.confidenceThreshold = confidenceThreshold
    }

    /// Filter eligible AdWindows, create SkipCues, and persist via INSERT OR IGNORE.
    /// Returns the cues that were created (dedup is handled by cueHash UNIQUE constraint).
    func materialize(
        windows: [AdWindow],
        analysisAssetId: String,
        source: String = "preAnalysis"
    ) async throws -> [SkipCue] {
        let eligible = windows.filter { $0.confidence >= confidenceThreshold && $0.endTime > $0.startTime }
        let cues = eligible.map { window -> SkipCue in
            let hash = Self.computeCueHash(
                analysisAssetId: analysisAssetId,
                startTime: window.startTime,
                endTime: window.endTime
            )
            return SkipCue(
                id: UUID().uuidString,
                analysisAssetId: analysisAssetId,
                cueHash: hash,
                startTime: window.startTime,
                endTime: window.endTime,
                confidence: window.confidence,
                source: source,
                materializedAt: Date().timeIntervalSince1970,
                wasSkipped: false,
                userDismissed: false
            )
        }
        try await store.insertSkipCues(cues)
        return cues
    }

    /// Deterministic hash: rounds start/end to integer seconds so that
    /// windows at e.g. 12.3-45.7 and 12.8-45.2 produce the same hash.
    static func computeCueHash(analysisAssetId: String, startTime: Double, endTime: Double) -> String {
        let input = "\(analysisAssetId):\(Int(startTime.rounded())):\(Int(endTime.rounded()))"
        let digest = SHA256.hash(data: Data(input.utf8))
        return digest.map { String(format: "%02x", $0) }.joined()
    }
}
