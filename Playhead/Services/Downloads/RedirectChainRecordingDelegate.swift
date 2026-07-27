// RedirectChainRecordingDelegate.swift
// playhead-xsdz.71 (Signal 1): a minimal, behavior-preserving URLSession task
// delegate that records the HOST of each redirect hop an enclosure download
// follows, so the DAI-stitch classifier can identify the show's ad-stitch
// network (see `DAIStitchClassifier`).
//
// BEHAVIOR-PRESERVING: the only delegate method implemented is the redirect
// callback, and it returns the PROPOSED new request unchanged — exactly what
// URLSession does by default when no delegate is set. It records the hop host as
// a side effect and never alters, blocks, or rewrites the request. Attaching
// this delegate to a download therefore does not change what bytes are fetched
// or how redirects are followed; it only observes.

import Foundation
import os

/// Records the ordered hop hosts of an enclosure download's redirect chain.
///
/// The instance is seeded with the initial request host and appends each
/// redirect target host as `willPerformHTTPRedirection` fires on the session's
/// delegate queue. State is guarded by an `OSAllocatedUnfairLock` so `hopHosts`
/// can be read on the actor after the download completes while callbacks may
/// still be arriving on the delegate queue.
final class RedirectChainRecordingDelegate: NSObject, URLSessionTaskDelegate, @unchecked Sendable {
    private let hostsLock: OSAllocatedUnfairLock<[String]>

    /// - Parameter initialHost: the host of the original enclosure URL, recorded
    ///   as the first hop so the chain is complete from the start.
    init(initialHost: String?) {
        let seed = initialHost.map { [$0] } ?? []
        self.hostsLock = OSAllocatedUnfairLock(initialState: seed)
    }

    /// The ordered hop hosts observed so far: the initial host first, then each
    /// redirect target. Consecutive duplicates are collapsed. Safe to read at
    /// any time.
    var hopHosts: [String] {
        hostsLock.withLock { $0 }
    }

    func urlSession(
        _ session: URLSession,
        task: URLSessionTask,
        willPerformHTTPRedirection response: HTTPURLResponse,
        newRequest request: URLRequest
    ) async -> URLRequest? {
        if let host = request.url?.host, !host.isEmpty {
            hostsLock.withLock { hosts in
                if hosts.last != host { hosts.append(host) }
            }
        }
        // Return the proposed request so the redirect is followed exactly as the
        // default (no-delegate) URLSession behavior — this delegate only records.
        return request
    }
}
