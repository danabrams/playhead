// BackgroundSessionIdentifier.swift
// Identifiers, URLError → InternalMissCause mapping, and the
// `WorkJournalRecording` shim that `DownloadManager` uses to emit
// finalized/failed events from background session delegate callbacks.
//
// This file is the public surface of playhead-24cm ("dual background
// URLSession configurations"). The identifiers are split into
// user-initiated (`interactive`) and auto-download (`maintenance`)
// lanes, plus a legacy identifier kept alive for one release cycle so
// in-flight resume data from the previous single-session build drains
// rather than being orphaned.

import Foundation

// MARK: - BackgroundSessionIdentifier

/// Identifiers for the three background URLSession configurations the
/// download manager instantiates. The legacy id is retained during the
/// rollout window so transfers started by the previous build can finish;
/// new work is routed to `interactive` (user-tapped downloads) or
/// `maintenance` (subscription/pre-cache auto-downloads).
enum BackgroundSessionIdentifier {
    /// User-initiated downloads. `isDiscretionary = false` so the OS
    /// runs them as soon as possible.
    static let interactive = "com.playhead.transfer.interactive"

    /// Subscription auto-downloads. `isDiscretionary = true` so the OS
    /// can batch / defer to Wi-Fi + charging windows.
    static let maintenance = "com.playhead.transfer.maintenance"

    /// Pre-24cm single-session identifier. Kept alive for one release
    /// cycle to drain resume data from prior builds; scheduled for
    /// removal in a follow-up bead.
    static let legacy = "com.playhead.episode-downloads"

    /// Returns `true` if `identifier` is one of the three background
    /// session identifiers the app owns. Used by
    /// `handleEventsForBackgroundURLSession` routing.
    static func isKnown(_ identifier: String) -> Bool {
        identifier == interactive || identifier == maintenance || identifier == legacy
    }
}

// MARK: - URLError → InternalMissCause

extension InternalMissCause {
    /// Map a `URLError` raised by a background URLSession task into the
    /// engine-side miss cause. The mapping is intentionally narrow:
    ///
    ///   * `.notConnectedToInternet`, `.networkConnectionLost`,
    ///     `.dataNotAllowed` (radio off) → `.noNetwork`
    ///   * `.internationalRoamingOff`, plus `cellularDenied` user
    ///     preference → `.wifiRequired`
    ///   * `.timedOut`, `.backgroundSessionWasDisconnected` → `.taskExpired`
    ///   * anything else → `.pipelineError`
    ///
    /// This keeps the mapping faithful to how the miss appears to the
    /// runtime even if the raw URLError is less precise.
    static func fromURLError(_ error: URLError) -> InternalMissCause {
        switch error.code {
        case .notConnectedToInternet,
             .networkConnectionLost,
             .dataNotAllowed:
            return .noNetwork
        case .internationalRoamingOff:
            return .wifiRequired
        case .timedOut,
             .backgroundSessionWasDisconnected:
            return .taskExpired
        default:
            return .pipelineError
        }
    }

    /// Map an arbitrary `Error` delivered to
    /// `urlSession(_:task:didCompleteWithError:)`. URLError instances
    /// route through `fromURLError`; anything else collapses to
    /// `.pipelineError`.
    static func fromTaskError(_ error: Error) -> InternalMissCause {
        if let urlError = error as? URLError {
            return .fromURLError(urlError)
        }
        return .pipelineError
    }
}

// MARK: - WorkJournalRecording

/// Forward-declared surface for playhead-uzdq's work journal. 24cm
/// consumes this protocol from the download delegate so it can emit
/// finalized/failed events without taking a dependency on the uzdq
/// types (which land on a parallel branch).
///
/// The default in-process binding (`NoopWorkJournalRecorder`) swallows
/// events; uzdq will register a real recorder via
/// `DownloadManager.setWorkJournalRecorder(_:)`.
protocol WorkJournalRecording: Sendable {
    /// Record that the background transfer for `episodeId` finished
    /// successfully and its artifact is in place.
    func recordFinalized(episodeId: String) async

    /// Record that the background transfer for `episodeId` failed with
    /// the given internal cause.
    func recordFailed(episodeId: String, cause: InternalMissCause) async

    /// Record that the background transfer for `episodeId` failed with
    /// the given internal cause, attaching the `SliceMetadata` JSON
    /// blob the emission site constructed (see
    /// `SliceCompletionInstrumentation.recordFailed(...)`).
    ///
    /// REQUIRED — no default implementation. A prior revision of this
    /// protocol shipped a default that forwarded to the metadata-less
    /// overload, silently dropping the JSON payload. That default was
    /// removed in playhead-1nl6 review because the blob is load-bearing
    /// for downstream consumers (WorkJournal diagnostics, device-class
    /// aggregation); a silent drop on the default path would swallow
    /// data the spec says every terminal row must carry. Every
    /// conformer must decide explicitly whether to persist the blob
    /// (`HyhtRecorder`, `RecordingWorkJournal`) or no-op it
    /// (`NoopWorkJournalRecorder`) — but that choice must be visible at
    /// the conformer, not hidden behind a protocol default.
    func recordFailed(
        episodeId: String,
        cause: InternalMissCause,
        metadataJSON: String
    ) async

    /// Record that the background transfer for `episodeId` was pre-empted
    /// (force-quit relaunch, explicit pause, higher-priority demand) and
    /// may be resumed later. `cause` tags the reason (e.g.
    /// `.appForceQuitRequiresRelaunch` for playhead-hyht's cold-launch
    /// scan). `metadataJSON` carries caller-specific context (transfer
    /// id, bytes written, suspension timestamp) for the WorkJournal row's
    /// metadata column — the recorder does not parse it.
    ///
    /// REQUIRED — no default implementation. A prior revision shipped a
    /// default that silently swallowed the event; that default was
    /// removed in playhead-1nl6 review for the same reason as
    /// ``recordFailed(episodeId:cause:metadataJSON:)`` — conformers
    /// must decide explicitly whether to persist, not inherit a silent
    /// drop.
    func recordPreempted(
        episodeId: String,
        cause: InternalMissCause,
        metadataJSON: String
    ) async
}

/// Default no-op binding used until playhead-uzdq wires a real recorder.
/// Kept `final` + `Sendable` so it can be stored in an actor.
final class NoopWorkJournalRecorder: WorkJournalRecording, Sendable {
    func recordFinalized(episodeId: String) async {}
    func recordFailed(episodeId: String, cause: InternalMissCause) async {}
    /// Explicit no-op: the Noop binding is the "until a real recorder
    /// is wired" stand-in, so dropping the blob here is deliberate and
    /// documented at the conformer — not hidden behind a protocol
    /// default (see ``WorkJournalRecording.recordFailed(...metadataJSON:)``
    /// docs).
    func recordFailed(
        episodeId: String,
        cause: InternalMissCause,
        metadataJSON: String
    ) async {}
    func recordPreempted(
        episodeId: String,
        cause: InternalMissCause,
        metadataJSON: String
    ) async {}
}
