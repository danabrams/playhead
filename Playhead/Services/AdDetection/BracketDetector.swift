// BracketDetector.swift
// ef2.3.6: Finite-state envelope scanner that identifies music-bed brackets
// around candidate ad regions. SHADOW MODE only — not wired into live skip.
//
// The state machine is deterministic: given the same FeatureWindow sequence
// and candidate region, it always produces the same BracketEvidence.
//
// States:  idle → onsetCandidate → bedSustained → offsetCandidate → bracketed
// Inputs:  acoustic feature windows (energy, spectral, music probability)
// Output:  BracketEvidence with template classification and coarse confidence

import Foundation

// MARK: - BracketTemplate

/// Template families describing the envelope shape around an ad bracket.
enum BracketTemplate: String, Sendable, Equatable, CaseIterable {
    /// Sharp music onset, sustained bed under speech, dry voice exit.
    case stingInBedDryOut
    /// Dry voice entry, music sting at exit.
    case dryInStingOut
    /// Hard music cut-in, gradual fade-out.
    case hardInFadeOut
    /// Similar onset/offset patterns (symmetric jingle pair).
    case symmetricBracket
    /// Onset detected but offset unclear.
    case partialOnset
    /// Offset detected but onset unclear.
    case partialOffset
}

// MARK: - BracketEvidence

/// Output of the bracket detector for a single candidate region.
struct BracketEvidence: Sendable, Equatable {
    /// Estimated bracket start time (seconds).
    let onsetTime: Double
    /// Estimated bracket end time (seconds).
    let offsetTime: Double
    /// Classified envelope template.
    let templateClass: BracketTemplate
    /// Coarse confidence score (0.0-1.0).
    let coarseScore: Double
    /// Per-show musicBracketTrust at the time of detection.
    let showTrust: Double
}

// MARK: - BracketDetector

/// Deterministic finite-state envelope scanner for music-bed brackets.
/// Pure computation on value types — no actor needed.
enum BracketDetector {

    // MARK: - State machine

    enum State: String, Sendable, Equatable {
        case idle
        case onsetCandidate
        case bedSustained
        case offsetCandidate
        case bracketed
    }

    // MARK: - Configuration

    struct Config: Sendable, Equatable {
        /// Minimum music onset score to transition idle → onsetCandidate.
        let onsetScoreThreshold: Double
        /// Minimum music probability to sustain a bed (onsetCandidate → bedSustained).
        let bedSustainMusicProbThreshold: Double
        /// Minimum number of consecutive windows above bedSustainMusicProbThreshold
        /// to confirm bed sustain.
        let minBedSustainWindows: Int
        /// Minimum music offset score to transition bedSustained → offsetCandidate.
        let offsetScoreThreshold: Double
        /// Maximum gap (in windows) allowed between onset and first bed window.
        let maxOnsetToBedGap: Int
        /// Minimum RMS ratio (onset window / local mean) for a sharp onset.
        let sharpOnsetRMSRatio: Double
        /// Threshold for classifying a fade-out (gradual RMS decline over N windows).
        let fadeOutRMSDeclineRate: Double
        /// How many trailing windows to check for fade-out pattern.
        let fadeOutWindowCount: Int

        static let `default` = Config(
            onsetScoreThreshold: 0.3,
            bedSustainMusicProbThreshold: 0.25,
            minBedSustainWindows: 2,
            offsetScoreThreshold: 0.3,
            maxOnsetToBedGap: 2,
            sharpOnsetRMSRatio: 1.5,
            fadeOutRMSDeclineRate: 0.15,
            fadeOutWindowCount: 3
        )
    }

    // MARK: - Internal tracking

    /// Mutable context carried across window processing steps.
    private struct ScanContext {
        var state: State = .idle
        var onsetWindowIndex: Int?
        var onsetTime: Double?
        var onsetRMS: Double?
        var onsetMusicOnsetScore: Double = 0
        var bedStartIndex: Int?
        var bedWindowCount: Int = 0
        var offsetWindowIndex: Int?
        var offsetTime: Double?
        var offsetMusicOffsetScore: Double = 0
        /// RMS values in the offset region for fade-out detection.
        var trailingRMSValues: [Double] = []
    }

    // MARK: - Public API

    /// Scan for bracket evidence around a candidate ad region.
    ///
    /// - Parameters:
    ///   - candidateStart: Start time of the candidate ad region (seconds).
    ///   - candidateEnd: End time of the candidate ad region (seconds).
    ///   - windows: All feature windows for the episode, sorted by startTime.
    ///   - showTrust: Per-show musicBracketTrust value (from MusicBracketTrustStore).
    ///   - config: Detection thresholds.
    /// - Returns: BracketEvidence if a bracket pattern is detected, nil otherwise.
    static func scanForBrackets(
        around candidateStart: Double,
        candidateEnd: Double,
        using windows: [FeatureWindow],
        showTrust: Double,
        config: Config = .default
    ) -> BracketEvidence? {
        guard !windows.isEmpty else { return nil }

        let sorted = windows.sorted { $0.startTime < $1.startTime }

        // Select windows that overlap or are near the candidate region.
        // Expand search by a few windows on each side to catch onset/offset.
        let windowDuration = sorted.count >= 2
            ? sorted[1].startTime - sorted[0].startTime
            : 2.0
        let searchMargin = windowDuration * 5
        let searchStart = candidateStart - searchMargin
        let searchEnd = candidateEnd + searchMargin

        let relevantWindows = sorted.filter {
            $0.endTime >= searchStart && $0.startTime <= searchEnd
        }

        guard relevantWindows.count >= 3 else { return nil }

        // Compute local mean RMS for amplitude-relative checks.
        let localMeanRMS = relevantWindows.map(\.rms).reduce(0, +)
            / Double(relevantWindows.count)

        // Run the state machine forward through the relevant windows.
        var ctx = ScanContext()

        for (i, window) in relevantWindows.enumerated() {
            switch ctx.state {
            case .idle:
                if window.musicBedOnsetScore >= config.onsetScoreThreshold
                    && window.startTime <= candidateStart + windowDuration {
                    ctx.state = .onsetCandidate
                    ctx.onsetWindowIndex = i
                    ctx.onsetTime = window.startTime
                    ctx.onsetRMS = window.rms
                    ctx.onsetMusicOnsetScore = window.musicBedOnsetScore
                }

            case .onsetCandidate:
                let gapFromOnset = i - (ctx.onsetWindowIndex ?? i)
                if window.musicProbability >= config.bedSustainMusicProbThreshold {
                    ctx.state = .bedSustained
                    ctx.bedStartIndex = i
                    ctx.bedWindowCount = 1
                } else if gapFromOnset > config.maxOnsetToBedGap {
                    // Onset fizzled — reset.
                    ctx.state = .idle
                    ctx.onsetWindowIndex = nil
                    ctx.onsetTime = nil
                    ctx.onsetRMS = nil
                }

            case .bedSustained:
                if window.musicProbability >= config.bedSustainMusicProbThreshold {
                    ctx.bedWindowCount += 1
                }
                if window.musicBedOffsetScore >= config.offsetScoreThreshold
                    && ctx.bedWindowCount >= config.minBedSustainWindows
                    && window.startTime >= candidateEnd - windowDuration {
                    ctx.state = .offsetCandidate
                    ctx.offsetWindowIndex = i
                    ctx.offsetTime = window.endTime
                    ctx.offsetMusicOffsetScore = window.musicBedOffsetScore
                    ctx.trailingRMSValues = []
                }

            case .offsetCandidate:
                ctx.trailingRMSValues.append(window.rms)
                if ctx.trailingRMSValues.count >= config.fadeOutWindowCount {
                    ctx.state = .bracketed
                }

            case .bracketed:
                break // Terminal state.
            }
        }

        // Determine what we found.
        let hasOnset = ctx.onsetTime != nil
        let hasOffset = ctx.offsetTime != nil

        // If we reached .bracketed or .offsetCandidate, we have a full or partial bracket.
        guard hasOnset || hasOffset else { return nil }

        let onsetTime = ctx.onsetTime ?? candidateStart
        let offsetTime = ctx.offsetTime ?? candidateEnd

        // Classify the template.
        let template = classifyTemplate(
            ctx: ctx,
            localMeanRMS: localMeanRMS,
            hasOnset: hasOnset,
            hasOffset: hasOffset,
            config: config
        )

        // Compute coarse confidence.
        let coarseScore = computeCoarseScore(
            ctx: ctx,
            hasOnset: hasOnset,
            hasOffset: hasOffset,
            showTrust: showTrust
        )

        return BracketEvidence(
            onsetTime: onsetTime,
            offsetTime: offsetTime,
            templateClass: template,
            coarseScore: coarseScore,
            showTrust: showTrust
        )
    }

    // MARK: - Template classification

    private static func classifyTemplate(
        ctx: ScanContext,
        localMeanRMS: Double,
        hasOnset: Bool,
        hasOffset: Bool,
        config: Config
    ) -> BracketTemplate {
        guard hasOnset && hasOffset else {
            return hasOnset ? .partialOnset : .partialOffset
        }

        let onsetRMS = ctx.onsetRMS ?? 0
        let isSharpOnset = localMeanRMS > 0
            && onsetRMS / localMeanRMS >= config.sharpOnsetRMSRatio

        let isFadeOut = detectFadeOut(
            trailingRMS: ctx.trailingRMSValues,
            declineRate: config.fadeOutRMSDeclineRate
        )

        // Symmetric: both onset and offset scores are above threshold and
        // similar in magnitude (within 50% of each other).
        let onsetMag = ctx.onsetMusicOnsetScore
        let offsetMag = ctx.offsetMusicOffsetScore
        let isSymmetric: Bool
        if onsetMag > 0 && offsetMag > 0 {
            let ratio = min(onsetMag, offsetMag) / max(onsetMag, offsetMag)
            isSymmetric = ratio >= 0.5
        } else {
            isSymmetric = false
        }

        if isSharpOnset && isFadeOut {
            return .hardInFadeOut
        } else if isSharpOnset && !isFadeOut {
            return .stingInBedDryOut
        } else if !isSharpOnset && !isFadeOut && isSymmetric {
            return .symmetricBracket
        } else if !isSharpOnset && offsetMag > onsetMag {
            return .dryInStingOut
        } else {
            return .stingInBedDryOut // Default full bracket.
        }
    }

    // MARK: - Fade-out detection

    /// Returns true if the trailing RMS values show a consistent decline.
    private static func detectFadeOut(
        trailingRMS: [Double],
        declineRate: Double
    ) -> Bool {
        guard trailingRMS.count >= 2 else { return false }

        var declines = 0
        for i in 1..<trailingRMS.count {
            let prev = trailingRMS[i - 1]
            guard prev > 0 else { continue }
            let drop = (prev - trailingRMS[i]) / prev
            if drop >= declineRate {
                declines += 1
            }
        }

        // Majority of transitions must be declining.
        return declines >= (trailingRMS.count - 1 + 1) / 2
    }

    // MARK: - Coarse score

    /// Compute a confidence score based on state machine completeness and
    /// signal strength.
    private static func computeCoarseScore(
        ctx: ScanContext,
        hasOnset: Bool,
        hasOffset: Bool,
        showTrust: Double
    ) -> Double {
        var score = 0.0

        // Completeness: full bracket = 0.5 base, partial = 0.25.
        if hasOnset && hasOffset {
            score += 0.4
        } else {
            score += 0.15
        }

        // Signal strength from onset/offset scores.
        score += min(ctx.onsetMusicOnsetScore, 1.0) * 0.15
        score += min(ctx.offsetMusicOffsetScore, 1.0) * 0.15

        // Bed sustain: more windows = higher confidence, capped at contribution.
        let sustainBonus = min(Double(ctx.bedWindowCount) / 5.0, 1.0) * 0.15
        score += sustainBonus

        // Show trust modulates the final score.
        score *= (0.5 + 0.5 * showTrust)

        return min(max(score, 0.0), 1.0)
    }
}
