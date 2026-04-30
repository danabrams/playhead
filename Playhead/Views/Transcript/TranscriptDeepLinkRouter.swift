// TranscriptDeepLinkRouter.swift
// playhead-m8v7: pure router for incoming `playhead://episode/<id>?t=<sec>`
// URLs. Translates a URL into the action sequence
//
//   1. resolve `episodeId` → `Episode` (via SwiftData lookup; injected
//      as a closure so the router has zero dependency on the model
//      container).
//   2. play that episode via `PlayheadRuntime.playEpisode`.
//   3. seek to the URL's timestamp.
//
// Everything is closure-injected so unit tests can stand the router up
// without SwiftData / runtime.

import Foundation

// MARK: - TranscriptDeepLinkRouter

@MainActor
struct TranscriptDeepLinkRouter {

    /// Identifying handle the router hands back to its play closure.
    /// Today this is just the canonical episode key — boxing it in a
    /// type makes it self-documenting at the closure boundary and lets
    /// us add fields (e.g. resume position) without churning the
    /// router's public surface.
    struct ResolvedEpisode: Equatable, Sendable {
        let episodeId: String
    }

    /// Resolves a canonical episode key into a `ResolvedEpisode`, or
    /// `nil` when the id is unknown to the local SwiftData store.
    /// Production wires this to a `FetchDescriptor<Episode>` lookup.
    let resolveEpisode: @MainActor (_ episodeId: String) async -> ResolvedEpisode?

    /// Begins playback of the resolved episode. Production wires this
    /// to `PlayheadRuntime.playEpisode(_:)` (which itself takes an
    /// `Episode`, so the closure does the SwiftData re-fetch).
    let playEpisode: @MainActor (ResolvedEpisode) async -> Void

    /// Seeks to the URL's `t=` value. Production wires this to
    /// `PlayheadRuntime.seek(to:)`.
    let seek: @MainActor (TimeInterval) async -> Void

    /// Handle an incoming URL. Returns `true` when the URL matched the
    /// scheme AND the episode was resolved AND the action sequence
    /// dispatched; `false` for any rejection (wrong scheme, unknown
    /// episode). Production callers can use the boolean to fall
    /// through to other URL handlers if any are added later.
    @discardableResult
    func handle(url: URL) async -> Bool {
        guard let payload = TranscriptDeepLink.parse(url) else {
            return false
        }
        guard let resolved = await resolveEpisode(payload.episodeId) else {
            // Unknown id: log site is the caller's; here we just
            // signal "not handled" so the action sequence is skipped.
            return false
        }
        await playEpisode(resolved)
        await seek(payload.startTime)
        return true
    }
}
