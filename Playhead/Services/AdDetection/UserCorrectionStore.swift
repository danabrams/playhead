// UserCorrectionStore.swift
// Phase 5 (playhead-u4d): Protocol + no-op stub for user correction gestures.
//
// Phase 5 ships the protocol and a no-op implementation so the transcript overlay
// UI can wire up the "This isn't an ad" gesture without requiring Phase 7 persistence.
// Phase 7 will conform a real UserCorrectionStore implementation to this protocol
// and inject it through PlayheadRuntime, replacing the stub.
//
// The protocol is intentionally minimal — extend it in Phase 7 without breaking callers.

import Foundation

// MARK: - UserCorrectionStore

protocol UserCorrectionStore: Sendable {
    /// Record that the user vetoed a decoded span as not-an-ad.
    ///
    /// - Parameters:
    ///   - span: The DecodedSpan the user tapped "This isn't an ad" on.
    ///   - timeRange: The time range covered by the user's gesture (may be narrower than the full span).
    func recordVeto(span: DecodedSpan, timeRange: ClosedRange<Double>) async
}

// MARK: - NoOpUserCorrectionStore

/// No-op stub for Phase 5. Discards all corrections.
/// Phase 7 replaces this with a persistent implementation.
struct NoOpUserCorrectionStore: UserCorrectionStore {
    func recordVeto(span: DecodedSpan, timeRange: ClosedRange<Double>) async {
        // No-op: Phase 7 will persist the correction.
    }
}
