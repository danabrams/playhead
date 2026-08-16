// ManualVetoOutcome.swift
// playhead-zxqj — what happened to a "Dismiss ad" gesture, and why.
//
// The transcript veto (`SkipOrchestrator.revertByTimeRange`) answers a `Bool`.
// It has roughly a dozen ways to answer `false`, `TranscriptPeekView
// .submitNotAdChunks` swallows every one of them with a bare `guard … else
// { return }`, and none of them writes anything anywhere. So a refused dismiss
// and an untapped button leave an IDENTICAL database and an IDENTICAL screen.
//
// That is what made Dan's 2026-08-15 report ("i had a number of times where i
// couldnt dismiss an ad usually when i had already dismissed part of the window
// (or confirmed part)") undecidable from the device pull: the two episodes he
// named carry zero `manualVeto` corrections, which is equally consistent with
// "every attempt was refused" and with "he never opened the transcript". A
// value that names an ABSENCE was going to be read as evidence of a presence —
// this repo's standing defect class — so the fix is to stop inferring the
// gesture from its own side effects and record it.

import Foundation

/// The terminal disposition of one transcript "Dismiss ad" gesture.
///
/// Exhaustive over `revertByTimeRange`'s exits: every `return` in that method
/// names one of these. A new exit without a case is a compile error at the
/// call site, which is the point — the previous instrument for this seam was
/// nothing at all, and a partial instrument is how a refusal comes to look
/// like a success.
enum ManualVetoOutcome: String, Sendable, Hashable, CaseIterable {

    /// Committed. Windows may or may not have been reverted (see
    /// `revertedWindows`) — a veto over material that carries no `ad_window`
    /// still records its `CorrectionEvent`, which is the durable artifact the
    /// detection pipeline learns from.
    case committed

    /// The gesture's own arguments were unusable: a non-finite or inverted
    /// range, a missing asset/episode identity, or a `correctionSpan` whose
    /// bit-pattern does not match the range it was submitted with.
    case refusedInvalidRequest

    /// The episode, asset or playback lifecycle the sheet captured is no longer
    /// the one playing. The honest answer to a stale gesture, and the one
    /// refusal here that is not a defect — but it must still be visible,
    /// because it is indistinguishable on screen from every other refusal.
    case refusedStaleContext

    /// A live suggest entry and a live managed entry disagreed about the same
    /// window id while the persisted read was unavailable. Reachable only in
    /// the degraded live-only path.
    case refusedLiveTargetConflict

    /// Retracting the recurrence/catalog evidence for a target threw, so
    /// nothing was written.
    case refusedRevocationFailed

    /// No `ad_window` overlapped the range AND no `decoded_span` did either —
    /// the app has no claim over that audio to retract.
    case refusedNothingToCorrect

    /// The durable transaction refused: a target row moved, vanished, or was
    /// already terminal, or the correction failed validation.
    case refusedDurableWriteRejected

    /// The durable write threw.
    case refusedPersistenceFailed

    /// Did the listener get what they asked for?
    var didCommit: Bool { self == .committed }
}

/// One audit row for a `ManualVetoOutcome`, in the shape
/// `AdWindowIngestCensus` established: a flat, sorted, greppable body carrying
/// no user content — asset ids are UUIDs and the range is a pair of offsets.
struct ManualVetoOutcomeAudit: Sendable, Hashable {

    let outcome: ManualVetoOutcome
    let analysisAssetId: String
    let startTime: Double
    let endTime: Double

    /// How many `ad_windows` rows the transaction reverted. Zero on a committed
    /// window-less veto, which is a real and healthy shape — see
    /// `ManualVetoOutcome.committed`.
    let revertedWindows: Int

    /// How many windows the LIVE session was holding over this range. Recorded
    /// beside `revertedWindows` because the gap between them is the quantity
    /// this bead is about: before playhead-zxqj a live entry the store would
    /// not accept refused the whole gesture, so `live > 0, reverted = 0,
    /// outcome = refusedDurableWriteRejected` is that defect's exact signature
    /// and stays readable if it ever returns by another route.
    let liveTargets: Int

    var auditDescription: String {
        [
            "outcome=\(outcome.rawValue)",
            "asset=\(analysisAssetId)",
            String(format: "range=%.3f-%.3f", startTime, endTime),
            "reverted=\(revertedWindows)",
            "live=\(liveTargets)",
        ].joined(separator: " ")
    }
}
