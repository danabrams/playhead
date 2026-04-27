// AudioFileDurationProbe.swift
// playhead-gyvb.2: cheap container-metadata probe that returns the actual
// duration of a downloaded audio file via `AVURLAsset.load(.duration)`.
//
// Background:
// Real-world incident, 2026-04-27 — feed-metadata `<itunes:duration>` was
// off by up to 13.8× on some libsyn/flightcast feeds (declared 704s, actual
// 9700s). The disposition reducer reads `episodeDurationSec` as truth in
// coverage-ratio gates; if transcript/feature watermarks exceed the (wrong)
// declared duration, classification logic is permanently confused.
//
// User directive: "Once we have the real runtime from the file that should
// be the source of truth."
//
// This helper is the source of that truth. It is intentionally minimal:
// no decode, no fallback to track-by-track scanning — `AVURLAsset` reads
// the container header (~tens of KB) and returns the duration the file
// actually has. If the file is not an audio file or the asset reader
// cannot interpret it, returns `nil` (not throws).

@preconcurrency import AVFoundation
import Foundation
import OSLog

/// Cheap, non-throwing duration probe for a local audio file.
///
/// - Reads container metadata via `AVURLAsset.load(.duration)`. No full
///   decode; no resampling; no track enumeration beyond what AVFoundation
///   needs to interpret the duration field.
/// - Returns `nil` (not throws) on any failure path: missing file,
///   non-audio container, indeterminate / non-finite duration. Callers
///   should treat `nil` as "no information" — they MUST NOT overwrite an
///   existing persisted duration with `nil`.
/// - Returns a strictly-positive, finite `TimeInterval` (seconds) on
///   success. Zero is rejected (treated as "indeterminate") because
///   `analysis_assets.episodeDurationSec` semantics treat 0 as missing.
enum AudioFileDurationProbe {

    private static let logger = Logger(subsystem: "com.playhead", category: "AudioFileDurationProbe")

    /// Probe the duration of `fileURL` via container metadata.
    ///
    /// Returns `nil` if:
    /// - The URL is not a local `file://` URL.
    /// - No file exists at the URL.
    /// - `AVURLAsset.load(.duration)` throws or returns a non-finite /
    ///   non-positive value (e.g. live streams report `kCMTimeIndefinite`).
    static func probeDuration(at fileURL: URL) async -> TimeInterval? {
        guard fileURL.isFileURL else {
            return nil
        }
        guard FileManager.default.fileExists(atPath: fileURL.path) else {
            return nil
        }

        let asset = AVURLAsset(url: fileURL)
        do {
            let cmTime = try await asset.load(.duration)
            // Reject indeterminate / negative-infinity / zero durations.
            guard cmTime.isValid, !cmTime.isIndefinite else {
                return nil
            }
            let seconds = CMTimeGetSeconds(cmTime)
            guard seconds.isFinite, seconds > 0 else {
                return nil
            }
            return seconds
        } catch {
            logger.debug("probeDuration failed for \(fileURL.lastPathComponent, privacy: .public): \(String(describing: error), privacy: .public)")
            return nil
        }
    }
}
