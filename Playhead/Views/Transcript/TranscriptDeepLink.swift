// TranscriptDeepLink.swift
// playhead-m8v7: pure encode/parse for the `playhead://episode/<id>?t=<sec>`
// deep-link scheme used by the transcript-share feature. Living
// alongside the rest of the transcript view code because that is the
// only producer; the consumer (URL handler) lives in `App/`.
//
// Wire format (kept stable so external links keep working):
//   scheme  = "playhead"
//   host    = "episode"
//   path    = "/<percent-encoded episodeId>"
//   query   = "t=<integer-seconds>"  (optional; defaults to 0 on parse)

import Foundation

// MARK: - TranscriptDeepLink

enum TranscriptDeepLink {

    /// Wire constants. Pinned here so a future scheme change has one
    /// edit site.
    static let scheme = "playhead"
    static let host = "episode"
    static let timeQueryKey = "t"

    /// Parsed payload from a deep link URL.
    struct Payload: Equatable, Sendable {
        let episodeId: String
        /// Integer-seconds time offset from the start of the episode.
        /// `0` when the URL omitted the `t=` query parameter.
        let startTime: TimeInterval
    }

    // MARK: - Encoding

    /// Build a `playhead://episode/<id>?t=<seconds>` URL. Negative times
    /// are clamped to zero; fractional seconds are rounded to the
    /// nearest integer.
    static func url(episodeId: String, startTime: TimeInterval) -> URL {
        let clampedSeconds = max(0, Int(startTime.rounded()))

        var components = URLComponents()
        components.scheme = scheme
        components.host = host
        // URLComponents.path expects a literal string; setting it via
        // `.path` performs the percent-encoding for path-allowed chars.
        // We prepend a slash so the absolute URL form is correct.
        components.path = "/" + episodeId
        components.queryItems = [
            URLQueryItem(name: timeQueryKey, value: String(clampedSeconds))
        ]

        // Falling back to a manual format only if URLComponents refuses
        // to compose a URL — the inputs above cannot produce that
        // because we always write a non-empty scheme and host. Use
        // `precondition` so a regression here surfaces in DEBUG builds
        // and the production fallback is a never-empty URL.
        guard let result = components.url else {
            preconditionFailure("TranscriptDeepLink.url failed to compose for episodeId=\(episodeId), startTime=\(startTime)")
        }
        return result
    }

    // MARK: - Parsing

    /// Inverse of `url(episodeId:startTime:)`. Returns `nil` for any
    /// URL whose scheme/host/path doesn't match the wire format, or
    /// whose `t` parameter is non-numeric. A missing `t` parameter is
    /// permissive: the payload defaults to `startTime: 0`.
    static func parse(_ url: URL) -> Payload? {
        guard let components = URLComponents(url: url, resolvingAgainstBaseURL: false) else {
            return nil
        }
        guard components.scheme == scheme, components.host == host else {
            return nil
        }

        // The path is "/<percent-decoded id>". `URLComponents.path`
        // returns the percent-DECODED form, so the round-trip lands
        // back on the original episode id even when special chars were
        // encoded on the wire.
        let path = components.path
        guard path.hasPrefix("/") else { return nil }
        let id = String(path.dropFirst())
        guard !id.isEmpty else { return nil }

        // Default to `0` when no `t=` is present; reject when present
        // but non-numeric (catches typos, never-existed URLs).
        var startTime: TimeInterval = 0
        if let tValue = components.queryItems?.first(where: { $0.name == timeQueryKey })?.value {
            guard let parsed = Int(tValue) else { return nil }
            startTime = TimeInterval(max(0, parsed))
        }

        return Payload(episodeId: id, startTime: startTime)
    }
}
