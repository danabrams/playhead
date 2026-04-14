// SpeakerLabelProvider.swift
// Protocol abstracting speaker label sources for ad boundary detection.
//
// Phase B (B7): Speaker labels can come from multiple sources:
//   1. Validated ASR speaker labels (SFSpeechRecognitionResult.speechRecognitionMetadata.speakerLabels — iOS 26+, unverified)
//   2. Offline diarization / speaker embedding change detector (future)
//   3. Acoustic turn-change proxy (Phase A fallback: pause + spectral shift + timbre)
//
// This protocol lets the pipeline consume whichever source is available,
// with graceful fallback to the acoustic proxy when no validated labels exist.

import Foundation

// MARK: - SpeakerLabel

/// A speaker label assignment for a time range.
struct SpeakerLabel: Sendable, Equatable {
    /// Speaker identifier (0-indexed). Nil means "unknown / no label available."
    let speakerId: Int?
    /// Start time in seconds (episode-relative).
    let startTime: Double
    /// End time in seconds (episode-relative).
    let endTime: Double
}

// MARK: - SpeakerChangeEvent

/// A detected speaker change at a specific time.
struct SpeakerChangeEvent: Sendable, Equatable {
    /// Time of the speaker change (seconds, episode-relative).
    let time: Double
    /// Speaker ID before the change (nil if unknown).
    let fromSpeakerId: Int?
    /// Speaker ID after the change (nil if unknown).
    let toSpeakerId: Int?
    /// Confidence in this being a real speaker change (0.0 ... 1.0).
    let confidence: Double
}

// MARK: - SpeakerLabelProviderKind

/// Identifies the source of speaker labels for diagnostics and logging.
enum SpeakerLabelProviderKind: String, Sendable {
    /// Validated ASR speaker labels from speech recognition metadata.
    case validatedASR
    /// Acoustic proxy: pause + spectral shift + timbre change.
    case acousticProxy
}

// MARK: - SpeakerLabelProvider Protocol

/// Abstracts over speaker label sources so the pipeline can consume
/// whichever source is available. Implementations must be value-type
/// or Sendable for use in concurrent pipelines.
protocol SpeakerLabelProvider: Sendable {
    /// Which kind of provider this is, for diagnostics.
    var kind: SpeakerLabelProviderKind { get }

    /// Whether this provider has validated speaker labels (not just acoustic proxy).
    var hasValidatedLabels: Bool { get }

    /// Returns speaker labels for the given time range, if available.
    /// Returns an empty array when no labels are available (not an error).
    func speakerLabels(
        startTime: Double,
        endTime: Double
    ) -> [SpeakerLabel]

    /// Returns detected speaker changes within the given time range.
    func speakerChanges(
        startTime: Double,
        endTime: Double
    ) -> [SpeakerChangeEvent]

    /// Computes speakerChangeProxyScore for a feature window, given
    /// the previous and next windows for smoothing context.
    ///
    /// When validated labels are available, returns 1.0 at actual turn
    /// boundaries (smoothed +/-1 window). When not available, returns
    /// the acoustic proxy score.
    func speakerChangeProxyScore(
        for window: FeatureWindow,
        previousWindow: FeatureWindow?,
        nextWindow: FeatureWindow?
    ) -> Double

    /// Returns the speaker ID for the given time, if available.
    func speakerId(at time: Double) -> Int?
}

// MARK: - AcousticSpeakerChangeProvider

/// Fallback provider using the Phase A acoustic proxy:
/// pause probability + spectral flux + timbre change + RMS delta.
///
/// This is the existing acoustic turn-change heuristic. It produces
/// speakerChangeProxyScore values but never validated speaker IDs.
struct AcousticSpeakerChangeProvider: SpeakerLabelProvider {
    let kind: SpeakerLabelProviderKind = .acousticProxy
    let hasValidatedLabels: Bool = false

    func speakerLabels(startTime: Double, endTime: Double) -> [SpeakerLabel] {
        // Acoustic proxy does not produce speaker labels.
        []
    }

    func speakerChanges(startTime: Double, endTime: Double) -> [SpeakerChangeEvent] {
        // Acoustic proxy does not produce discrete change events.
        []
    }

    func speakerChangeProxyScore(
        for window: FeatureWindow,
        previousWindow: FeatureWindow?,
        nextWindow: FeatureWindow?
    ) -> Double {
        // Delegate to existing acoustic proxy — the score is already
        // computed and stored on the FeatureWindow by FeatureExtraction.
        window.speakerChangeProxyScore
    }

    func speakerId(at time: Double) -> Int? {
        // Acoustic proxy cannot identify speakers.
        nil
    }
}

// MARK: - ValidatedSpeakerLabelProvider

/// Provider backed by validated ASR speaker labels.
///
/// When iOS exposes `SFSpeechRecognitionResult.speechRecognitionMetadata.speakerLabels`,
/// this provider consumes those labels to produce:
///   - Actual speaker IDs per time range
///   - speakerChangeProxyScore of 1.0 at turn boundaries (smoothed +/-1 window)
///
/// If speaker labels are not available on the current OS/SDK, callers
/// should fall back to `AcousticSpeakerChangeProvider`.
///
/// - Note: Overlapping labels are not expected from ASR output. If labels
///   overlap in time, behavior of `speakerId(at:)` and `computeChanges`
///   is undefined.
struct ValidatedSpeakerLabelProvider: SpeakerLabelProvider {
    let kind: SpeakerLabelProviderKind = .validatedASR
    let hasValidatedLabels: Bool = true

    /// Weight applied to adjacent windows when smoothing turn boundary scores.
    static let adjacentWindowSmoothingWeight: Double = 0.4

    /// Sorted speaker labels covering the episode.
    private let labels: [SpeakerLabel]

    /// Pre-computed speaker change times for fast lookup.
    private let changes: [SpeakerChangeEvent]

    init(labels: [SpeakerLabel]) {
        self.labels = labels.sorted { $0.startTime < $1.startTime }
        self.changes = Self.computeChanges(from: self.labels)
    }

    func speakerLabels(startTime: Double, endTime: Double) -> [SpeakerLabel] {
        labels.filter { label in
            label.endTime > startTime && label.startTime < endTime
        }
    }

    func speakerChanges(startTime: Double, endTime: Double) -> [SpeakerChangeEvent] {
        changes.filter { $0.time >= startTime && $0.time <= endTime }
    }

    func speakerChangeProxyScore(
        for window: FeatureWindow,
        previousWindow: FeatureWindow?,
        nextWindow: FeatureWindow?
    ) -> Double {
        // Check if any speaker change falls within this window's time range,
        // or within the adjacent windows (+/-1 smoothing).
        let windowStart = window.startTime
        let windowEnd = window.endTime

        // Direct hit: speaker change in this window.
        let directHit = changes.contains { change in
            change.time >= windowStart && change.time <= windowEnd
        }
        if directHit { return 1.0 }

        // Smoothing: check adjacent windows for turn boundaries.
        // A speaker change in an adjacent window contributes a reduced score.
        let smoothingWeight = Self.adjacentWindowSmoothingWeight

        if let prev = previousWindow {
            let prevHit = changes.contains { change in
                change.time >= prev.startTime && change.time <= prev.endTime
            }
            if prevHit { return smoothingWeight }
        }

        if let next = nextWindow {
            let nextHit = changes.contains { change in
                change.time >= next.startTime && change.time <= next.endTime
            }
            if nextHit { return smoothingWeight }
        }

        return 0.0
    }

    func speakerId(at time: Double) -> Int? {
        // Find the label that covers this time. Labels are sorted by startTime.
        labels.last { label in
            label.startTime <= time && label.endTime > time
        }?.speakerId
    }

    // MARK: - Private

    private static func computeChanges(from sortedLabels: [SpeakerLabel]) -> [SpeakerChangeEvent] {
        guard sortedLabels.count >= 2 else { return [] }

        var changes: [SpeakerChangeEvent] = []
        for i in 1..<sortedLabels.count {
            let prev = sortedLabels[i - 1]
            let curr = sortedLabels[i]
            // A speaker change occurs when the speaker ID differs between
            // adjacent labels.
            if prev.speakerId != curr.speakerId {
                changes.append(SpeakerChangeEvent(
                    time: curr.startTime,
                    fromSpeakerId: prev.speakerId,
                    toSpeakerId: curr.speakerId,
                    confidence: 1.0
                ))
            }
        }
        return changes
    }
}

// MARK: - SpeakerLabelProviderFactory

/// Factory that selects the best available speaker label provider.
///
/// Checks for validated ASR speaker label availability at runtime.
/// Falls back to acoustic proxy when validated labels are not available.
///
/// ## Integration steps (when `isASRSpeakerLabelsAvailable` flips to `true`)
///
/// 1. **TranscriptEngineService** must populate `TranscriptChunk.speakerId`
///    from the ASR recognition results (e.g. `SFSpeechRecognitionResult`
///    `.speechRecognitionMetadata.speakerLabels`).
/// 2. **Feature extraction pipeline** must call
///    `SpeakerLabelProviderFactory.makeProvider(labels:)` with the populated
///    speaker labels converted to `[SpeakerLabel]`.
/// 3. The provider's `speakerChangeProxyScore(for:previousWindow:nextWindow:)`
///    output must be used to populate/override
///    `FeatureWindow.speakerChangeProxyScore` **before** the window reaches
///    `TimeBoundaryResolver`.
enum SpeakerLabelProviderFactory {

    /// Creates the best available provider for the given speaker labels.
    ///
    /// - Parameter labels: Speaker labels from ASR, if available.
    /// - Returns: A validated provider if labels are non-empty, otherwise acoustic fallback.
    static func makeProvider(labels: [SpeakerLabel]?) -> any SpeakerLabelProvider {
        if let labels, !labels.isEmpty {
            return ValidatedSpeakerLabelProvider(labels: labels)
        }
        return AcousticSpeakerChangeProvider()
    }

    /// Checks whether the current OS/SDK supports ASR speaker labels.
    ///
    /// iOS 26 may expose `SFSpeechRecognitionResult.speechRecognitionMetadata.speakerLabels`.
    /// This is speculative — the API may not exist yet. The check is structured
    /// so that when/if Apple ships this API, enabling it requires only updating
    /// this availability check and wiring the labels through TranscriptEngineService.
    ///
    /// Returns `false` until verified on a target build.
    static var isASRSpeakerLabelsAvailable: Bool {
        // NOTE: SFSpeechRecognitionResult.speechRecognitionMetadata.speakerLabels
        // is not verified to exist in the current SDK. When iOS 26 ships and
        // this API is confirmed:
        //
        // 1. Add `#if canImport(Speech)` guard
        // 2. Check `if #available(iOS 26, *)` at runtime
        // 3. Probe the API via reflection or direct call
        // 4. Set this to return true when all checks pass
        //
        // Until then, the pipeline uses AcousticSpeakerChangeProvider as fallback.
        false
    }
}
