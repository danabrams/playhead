// RediffRefetchSeams.swift
// playhead-xsdz.28: the environment seams the `RediffRefetchService` drives —
// episode enumeration, the ranged-GET rotation pre-check, the full re-fetch,
// the B-side fingerprint (OFF the hot actor), the outcome recorder, and the
// B-copy remover — plus their production conformers.
//
// The seams keep the whole re-fetch POLICY offline-testable: tests inject
// scripted samples/downloads and assert the pre-check skips non-rotators, the
// ≥24h gate holds, the B-copy is deleted, and bandwidth is accounted — without
// a network or a device. Production conformers wire URLSession + FileHandle +
// the xsdz.27 fingerprint extractor.
//
// NO HEAD / NO ETag ANYWHERE (spike §3/§7): the ranged sampler issues ONLY
// range GETs and reads the total length from `Content-Range`. There is no
// conditional-GET (`If-None-Match` / `If-Modified-Since`) and no HEAD request
// in this file — HEAD is broken on Acast and podtrac length-flaps seconds apart.

import Foundation
import os
import OSLog

// MARK: - Candidate value type

/// One episode the re-fetch sweep may act on. The enumerator snapshots the
/// download metadata + the durable re-fetch state so the sweep runs entirely on
/// `Sendable` values (no live SwiftData / model objects cross the actor).
struct RediffRefetchCandidate: Sendable, Equatable {
    /// `analysis_assets.id` — the key the played-copy fingerprint (A-side) and
    /// any resulting rediff slots are stored under (xsdz.27/.29).
    let assetId: String
    /// The current enclosure URL for the episode's audio. Re-resolved from the
    /// feed by the enumerator (spike §7: use the CURRENT enclosure URL, not a
    /// stale one) so an expired URL is skipped upstream.
    let enclosureURL: URL
    /// Unix seconds the played copy was downloaded — the ≥24h gate's baseline.
    let downloadedAt: Double
    /// The played copy's on-disk audio file — the LOCAL side of the pre-check.
    let localAudioURL: URL
    /// Durable re-fetch bookkeeping for this episode (backoff / retry budget).
    let attemptState: RediffRefetchPolicy.AttemptState
}

// MARK: - Remote sample

/// The remote copy's change-detection sample plus the bytes it cost to fetch
/// (head + tail, ~128 KB). Produced by a `RangedAudioSampling` conformer.
struct RemoteAudioSample: Sendable, Equatable {
    let fingerprint: RediffRefetchPolicy.AudioSampleFingerprint
    /// Bytes actually transferred for the sample (for bandwidth accounting).
    let bytesTransferred: Int
}

// MARK: - Seams

/// Snapshots the episodes eligible for a re-fetch sweep (downloaded copies with
/// a resolvable current enclosure URL) as `Sendable` value candidates.
protocol RediffRefetchEnumerating: Sendable {
    func candidates() async -> [RediffRefetchCandidate]
}

/// Fetches the remote head/tail sample via RANGE GETs ONLY (no HEAD, no ETag).
protocol RangedAudioSampling: Sendable {
    func sample(url: URL, headBytes: Int, tailBytes: Int) async throws -> RemoteAudioSample
}

/// Computes the local (played-copy) head/tail sample from the on-disk file.
protocol LocalAudioSampling: Sendable {
    func sample(fileURL: URL, headBytes: Int, tailBytes: Int) throws -> RediffRefetchPolicy.AudioSampleFingerprint
}

/// Streams a full re-fetch of the changed copy to a TRANSIENT temp file. The
/// caller ALWAYS deletes it after fingerprinting — the B-copy is never
/// persisted. Returns the temp file URL + its byte count.
protocol FullEpisodeFetching: Sendable {
    func download(url: URL) async throws -> (fileURL: URL, byteCount: Int)
}

/// Decodes + resamples + fingerprints the B-side copy OFF the hot actor and
/// returns the subfingerprint stream. Conformers MUST NOT run the CPU-heavy
/// resample+fingerprint synchronously on a serial "hot" actor (xsdz.29 R5
/// residual): a full-episode resample would stall it. A plain `Sendable`
/// value-type conformer whose `async` body calls the pure extractor runs on the
/// generic concurrent executor, not any serial actor — that is the intended
/// shape.
protocol RediffBSideFingerprinting: Sendable {
    func fingerprint(fileURL: URL) async throws -> [UInt32]
}

/// Decodes an arbitrary audio file to mono 16 kHz PCM — the analysis pipeline's
/// decode rate (`AnalysisAudioService.targetSampleRate`). The one AVFoundation-
/// bound step; injected so the fingerprinter stays offline-testable. Live
/// wiring (reusing the existing decode path) lands with activation (xsdz.36).
protocol AudioFileDecoding: Sendable {
    func decodeMono16kHz(fileURL: URL) async throws -> [Float]
}

/// Records the terminal outcome of each candidate (skips, non-rotators,
/// rotations, failures) for dogfood accounting AND to persist the advanced
/// `AttemptState`. Default conformer just logs.
protocol RediffRefetchRecording: Sendable {
    func recordOutcome(_ outcome: RediffRefetchPolicy.Outcome) async
}

/// Removes the transient B-copy temp file. A seam (not a bare `FileManager`
/// call) so a test can assert removal WITHOUT a filesystem AND so the real
/// FileManager remover can be exercised against a real temp file.
protocol RediffTempFileRemoving: Sendable {
    func remove(_ fileURL: URL)
}

// MARK: - Production conformers

/// URLSession-backed ranged sampler. Issues exactly two GETs — `bytes=0-…` for
/// the head (reading the total length from `Content-Range`) and
/// `bytes=(total-tail)-…` for the tail — and NEVER a HEAD or conditional GET.
/// WiFi is enforced by `allowsCellularAccess = false` (the BGTask supplies the
/// charging + network-present gate; this pins the WiFi half of the policy).
struct URLSessionRangedAudioSampler: RangedAudioSampling {
    let session: URLSession

    init(session: URLSession = URLSessionRangedAudioSampler.makeWiFiOnlySession()) {
        self.session = session
    }

    /// A WiFi-and-not-constrained URLSession for rediff traffic (spike §5:
    /// "~1 GB/week over WiFi is acceptable … unacceptable on cellular").
    static func makeWiFiOnlySession() -> URLSession {
        let config = URLSessionConfiguration.ephemeral
        config.allowsCellularAccess = false
        config.allowsConstrainedNetworkAccess = false
        config.allowsExpensiveNetworkAccess = false
        return URLSession(configuration: config)
    }

    enum SampleError: Error, Equatable {
        case notPartialContent(status: Int)
        case missingContentRange
        case unparsableTotalLength(String)
    }

    func sample(url: URL, headBytes: Int, tailBytes: Int) async throws -> RemoteAudioSample {
        // HEAD request (bytes=0-(headBytes-1)) → also yields the total length.
        let (headData, total) = try await rangedGet(url: url, start: 0, length: headBytes, expectContentRange: true)
        let totalLength = try requireTotal(total)

        // TAIL request. Clamp the start for a file smaller than the tail window
        // so head and tail may overlap (deterministic; both sides use the same
        // clamp, so equal copies still compare equal). Episodes are MB-scale so
        // this is theoretical.
        let tailStart = max(0, totalLength - Int64(tailBytes))
        let tailLength = Int(totalLength - tailStart)
        let (tailData, _) = try await rangedGet(url: url, start: tailStart, length: tailLength, expectContentRange: false)

        let fingerprint = RediffRefetchPolicy.sampleFingerprint(
            head: headData,
            tail: tailData,
            totalLength: totalLength
        )
        return RemoteAudioSample(
            fingerprint: fingerprint,
            bytesTransferred: headData.count + tailData.count
        )
    }

    /// One range GET. Returns the body bytes and, when `expectContentRange`, the
    /// parsed total length from `Content-Range: bytes A-B/TOTAL`.
    private func rangedGet(
        url: URL,
        start: Int64,
        length: Int,
        expectContentRange: Bool
    ) async throws -> (Data, Int64?) {
        var request = URLRequest(url: url)
        request.httpMethod = "GET"
        request.allowsCellularAccess = false
        let end = start + Int64(max(0, length - 1))
        request.setValue("bytes=\(start)-\(end)", forHTTPHeaderField: "Range")

        let (data, response) = try await session.data(for: request)
        guard let http = response as? HTTPURLResponse else {
            throw SampleError.notPartialContent(status: -1)
        }
        // A CDN honoring the range replies 206. (Anything else — 200 full body,
        // 416, redirect loop — is a range failure; the caller treats the whole
        // candidate as failed rather than trusting a mis-sized sample.)
        guard http.statusCode == 206 else {
            throw SampleError.notPartialContent(status: http.statusCode)
        }
        guard expectContentRange else { return (data, nil) }
        guard let contentRange = http.value(forHTTPHeaderField: "Content-Range") else {
            throw SampleError.missingContentRange
        }
        return (data, try Self.parseTotalLength(contentRange))
    }

    private func requireTotal(_ total: Int64?) throws -> Int64 {
        guard let total, total > 0 else { throw SampleError.missingContentRange }
        return total
    }

    /// Parse the total length from `Content-Range: bytes 0-65535/84496614` (the
    /// part after the last `/`). Throws for a missing slash or an unknown (`*`)
    /// total — the length signal is the ranged GET's `Content-Range`, NOT HEAD.
    static func parseTotalLength(_ contentRange: String) throws -> Int64 {
        guard let slash = contentRange.lastIndex(of: "/") else {
            throw SampleError.unparsableTotalLength(contentRange)
        }
        let totalPart = contentRange[contentRange.index(after: slash)...]
            .trimmingCharacters(in: .whitespaces)
        guard totalPart != "*", let total = Int64(totalPart) else {
            throw SampleError.unparsableTotalLength(contentRange)
        }
        return total
    }
}

/// URLSession-backed full re-fetch. Streams the whole episode to a UNIQUE temp
/// file the caller owns and deletes; WiFi-only, matching the sampler.
struct URLSessionFullEpisodeFetcher: FullEpisodeFetching {
    let session: URLSession

    init(session: URLSession = URLSessionRangedAudioSampler.makeWiFiOnlySession()) {
        self.session = session
    }

    enum FetchError: Error, Equatable {
        case notOK(status: Int)
    }

    func download(url: URL) async throws -> (fileURL: URL, byteCount: Int) {
        var request = URLRequest(url: url)
        request.httpMethod = "GET"
        request.allowsCellularAccess = false

        let fileManager = FileManager.default
        let (tempURL, response) = try await session.download(for: request)
        if let http = response as? HTTPURLResponse, !(200...299).contains(http.statusCode) {
            try? fileManager.removeItem(at: tempURL)
            throw FetchError.notOK(status: http.statusCode)
        }
        // Move OUT of the URLSession-owned temp (which the system reclaims) into
        // a location WE control and delete after fingerprinting.
        let destination = fileManager.temporaryDirectory
            .appendingPathComponent("rediff-bcopy-\(UUID().uuidString)")
        try? fileManager.removeItem(at: destination)
        try fileManager.moveItem(at: tempURL, to: destination)
        return (destination, Self.fileByteCount(at: destination))
    }

    /// Byte size of a file, or 0 if unreadable.
    static func fileByteCount(at url: URL) -> Int {
        guard let attrs = try? FileManager.default.attributesOfItem(atPath: url.path),
              let size = attrs[.size] as? Int else { return 0 }
        return size
    }
}

/// FileHandle-backed local sampler. Reads the head/tail windows off the on-disk
/// played copy so its sample is directly comparable to the remote ranged one.
struct FileHandleLocalAudioSampler: LocalAudioSampling {

    init() {}

    func sample(fileURL: URL, headBytes: Int, tailBytes: Int) throws -> RediffRefetchPolicy.AudioSampleFingerprint {
        let handle = try FileHandle(forReadingFrom: fileURL)
        defer { try? handle.close() }
        let totalLength = Int64(URLSessionFullEpisodeFetcher.fileByteCount(at: fileURL))

        let head = try handle.read(upToCount: headBytes) ?? Data()

        let tailStart = max(0, totalLength - Int64(tailBytes))
        try handle.seek(toOffset: UInt64(tailStart))
        let tailCount = Int(totalLength - tailStart)
        let tail = try handle.read(upToCount: tailCount) ?? Data()

        return RediffRefetchPolicy.sampleFingerprint(head: head, tail: tail, totalLength: totalLength)
    }
}

/// Production B-side fingerprinter: decode → the EXACT xsdz.27 resample +
/// fingerprint extractor (`EpisodeFingerprintCapture.fingerprints`), so A-side
/// and B-side are fingerprinted by one versioned `(resampler + fingerprinter)`
/// unit. A plain `Sendable` struct: its `async` body runs on the generic
/// executor, NOT any serial hot actor (xsdz.29 R5 residual).
struct EpisodeCaptureBSideFingerprinter: RediffBSideFingerprinting {
    let decoder: any AudioFileDecoding

    func fingerprint(fileURL: URL) async throws -> [UInt32] {
        let mono16kHz = try await decoder.decodeMono16kHz(fileURL: fileURL)
        return EpisodeFingerprintCapture.fingerprints(mono16kHz: mono16kHz)
    }
}

/// FileManager-backed temp-file remover. Swallows a remove error (the file may
/// already be gone) but logs it so a persistent-B-copy regression is visible.
struct FileManagerTempFileRemover: RediffTempFileRemoving {
    private let logger = Logger(subsystem: "com.playhead", category: "RediffRefetch")

    init() {}

    func remove(_ fileURL: URL) {
        let fileManager = FileManager.default
        guard fileManager.fileExists(atPath: fileURL.path) else { return }
        do {
            try fileManager.removeItem(at: fileURL)
        } catch {
            logger.error("Failed to delete B-copy \(fileURL.lastPathComponent, privacy: .public): \(String(describing: error), privacy: .public)")
        }
    }
}

/// Default recorder: logs each outcome (bandwidth included) at info. Production
/// persistence of the advanced `AttemptState` lands with activation (xsdz.36);
/// this keeps the flag-OFF/shadow build observable without a store dependency.
struct LoggingRediffRefetchRecorder: RediffRefetchRecording {
    private let logger = Logger(subsystem: "com.playhead", category: "RediffRefetch")

    func recordOutcome(_ outcome: RediffRefetchPolicy.Outcome) async {
        switch outcome {
        case let .skippedIneligible(assetId, reason):
            logger.info("rediff-refetch skip assetId=\(assetId, privacy: .public) reason=\(String(describing: reason), privacy: .public)")
        case let .unchanged(assetId, cost, _):
            logger.info("rediff-refetch unchanged assetId=\(assetId, privacy: .public) precheckBytes=\(cost.precheckBytes, privacy: .public)")
        case let .rotated(assetId, cost, fingerprintCount, _):
            logger.info("rediff-refetch ROTATED assetId=\(assetId, privacy: .public) precheckBytes=\(cost.precheckBytes, privacy: .public) fullFetchBytes=\(cost.fullFetchBytes, privacy: .public) fpCount=\(fingerprintCount, privacy: .public)")
        case let .failed(assetId, cost, error):
            logger.error("rediff-refetch FAILED assetId=\(assetId, privacy: .public) bytes=\(cost.totalBytes, privacy: .public) error=\(error, privacy: .public)")
        }
    }
}
