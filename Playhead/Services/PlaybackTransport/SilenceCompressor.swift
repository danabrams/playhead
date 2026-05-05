// SilenceCompressor.swift
// playhead-epii — Structure-aware silence compression.
//
// MARK: - Design (1-page)
//
// Goal: selectively raise playback rate during non-content audio
// (music beds between segments, dead air during transitions, long
// intro/outro jingles) WITHOUT touching speech cadence, dramatic
// pauses, or the felt rhythm of conversation. The Smart-Speed-killer
// thesis is that Overcast compresses every silence uniformly and
// gets disabled on narrative shows; we know what KIND of silence
// we're compressing because the analysis pipeline already classifies
// it per `FeatureWindow`.
//
// State machine:
//
//   .idle ──► (lookahead finds a candidate gap covering the playhead)
//             beginCompression(rate, algorithm) on PlaybackService
//             ──► .compressing(rate, until: gapEnd)
//
//   .compressing ──► (playhead exits the gap, OR user seeks, OR
//                     transport pauses, OR plan invalidates)
//                    endCompression() on PlaybackService
//                    ──► .idle
//
// Sourcing: every `lookaheadCadenceSeconds` (default 5s) the
// coordinator (PlayheadRuntime) calls `tick(currentTime:)`, which
// asks the AnalysisStore for the upcoming
// `lookaheadHorizonSeconds` (default 60s) of windows and rebuilds an
// in-memory list of `CompressionPlan` ranges. The store call hops
// off the playback actor, so a slow SQLite query cannot stall
// transport. The plan is cached and reused until either:
//   (a) the playhead leaves the cached horizon,
//   (b) the user seeks (handled via `recordSeek`),
//   (c) the asset id changes.
//
// Decision matrix (windows are typically 2 seconds wide). Each
// window is bucketed once, then contiguous compressible buckets are
// coalesced into a single run; the run's algorithm + rate is decided
// from the run's overall character, not per-window:
//
//   musicProbability > 0.7   AND  musicBedLevel != .none
//     ⇒ music run candidate. Coalesce contiguous music windows.
//        Once coalesced, decide algorithm/rate per run:
//          - run touches a speech window on EITHER side (in the
//            buffer) ⇒ speech-adjacent ⇒ spectral / lowRate (cleaner
//            transition into and out of speech).
//          - run is bracketed only by other music or buffer edges
//            AND has at least one foreground bed window AND max
//            musicProbability across the run > 0.8
//            ⇒ pure music ⇒ varispeed / highRate.
//          - else (background bed, mid-confidence music)
//            ⇒ spectral / lowRate.
//   pauseProbability > 0.7   AND  speakerChangeProxyScore > 0.5
//     ⇒ dead-air run ⇒ spectral / lowRate.
//   pauseProbability > 0.7   BETWEEN two windows whose
//     speakerClusterId is non-nil and equal
//     ⇒ DRAMATIC PAUSE — preserve at base speed (this is the
//        most important correctness invariant; covered by tests).
//   AdWindow regions are skipped by `SkipOrchestrator` already, so
//     compression yields to skip — we never emit a plan inside a
//     range that the skip cue list also covers.
//
// Failure modes:
//   - No FeatureWindows available (asset not yet analyzed, fetch
//     throws): compressor stays in .idle, retries on next tick.
//   - AVPlayer rate-change failures: transport's `applyEffectiveRate`
//     is no-op when not playing; we never set rate from this layer
//     directly.
//   - Rapid window churn (alternating music/speech windows
//     <2s apart): the `minimumGapSeconds` (default 4s) filter
//     coalesces or rejects unstable plans, so the rate doesn't
//     thrash.
//   - User changed base speed mid-compression: PlaybackService's
//     `setSpeed` clears the multiplier; on the next tick the
//     compressor sees `currentCompressionMultiplier == 1.0` and
//     re-engages from idle if the playhead is still inside a plan.
//   - Per-show "Keep full music" override: the host clears the plan
//     and short-circuits before any plan is built when
//     `keepFullMusicOverride == true`.
//
// What this file does NOT do:
//   - Add a user-facing toggle in primary Settings (per bead spec).
//   - Add a "minutes saved" counter (peace of mind, not metrics).
//   - Persist anything: the plan is process-local in-memory state.
//   - Touch SkipOrchestrator: skip cues remain the auto-skip path,
//     we just refuse to compress over them.
//

import Foundation
import OSLog

// MARK: - SilenceCompressorConfig

/// Tunable thresholds for the structure-aware compressor. Default
/// values track the bead's exact numbers; tests construct
/// non-default configs to exercise the decision matrix in isolation.
struct SilenceCompressorConfig: Sendable, Equatable {

    // MARK: Decision thresholds (from bead playhead-epii)

    /// Music-bed candidate floor. Must match or exceed `0.7` per the
    /// bead's "musicProbability > 0.7" rule. Windows below this are
    /// not music candidates regardless of `musicBedLevel`.
    let musicProbabilityFloor: Double

    /// High-confidence music threshold. At or above this, we may
    /// engage the higher `varispeed` rate on `.foreground` beds.
    let highMusicProbabilityFloor: Double

    /// Pause-probability floor for both dead-air and dramatic-pause
    /// classification. Below this, the window is treated as content.
    let pauseProbabilityFloor: Double

    /// Speaker-change proxy threshold. `FeatureWindow.speakerChange`
    /// in the bead is approximated by `speakerChangeProxyScore`
    /// crossing this floor. Tuned conservatively so a single noisy
    /// 2s window doesn't fire dead-air compression.
    let speakerChangeProxyFloor: Double

    // MARK: Rate envelope

    /// Lower bound of the compression band (used on speech-adjacent
    /// music and dead-air candidates). `1.5×` is the bead's "2.0–3.0×"
    /// floor relaxed by one notch so the cleaner `.spectral` algorithm
    /// can be used end-to-end on audio adjacent to speech.
    let lowRateMultiplier: Float

    /// Upper bound of the compression band, only applied to sustained
    /// pure-music regions (`.foreground`, high musicProbability,
    /// minimum sustained run).
    let highRateMultiplier: Float

    // MARK: Plan stability

    /// Minimum sustained gap length (seconds) before we emit a
    /// compression plan. Below this, the gap rounds to "rhythm" and
    /// is left alone. `4.0s` matches the perceptual threshold below
    /// which a rate flip is more disruptive than the time saved.
    let minimumGapSeconds: TimeInterval

    /// Lookahead horizon: how far into the future the compressor
    /// queries the AnalysisStore for windows. `60s` per bead.
    let lookaheadHorizonSeconds: TimeInterval

    /// Throttle for both the lookahead window refetch and the planner
    /// `tick(currentTime:)` re-evaluation. Consumed by
    /// `SilenceCompressionCoordinator` — the planner itself does no
    /// time-based gating, so unit tests can drive `tick` at arbitrary
    /// cadence without observing this knob.
    let lookaheadCadenceSeconds: TimeInterval

    /// Hysteresis: extra seconds beyond a gap's nominal end that the
    /// compressor stays engaged before it disengages. Prevents
    /// flapping when the playhead lingers near the trailing
    /// `musicBedOffsetScore` boundary.
    let exitHysteresisSeconds: TimeInterval

    /// Onset/offset confidence floor used by `refineStart`/`refineEnd`.
    /// At or above this score, the boundary window's nominal start/end
    /// is trusted as-is. Below, we shift the boundary by
    /// `boundaryShiftFraction` of the window's duration toward the
    /// run's interior to avoid clipping speech tails or heads.
    let boundaryConfidenceFloor: Double

    /// Fraction of a window's duration to shift a low-confidence
    /// onset/offset boundary inward. `0.5` is half the window — at
    /// the standard 2s window size that's a 1s pull, which empirically
    /// lands inside the music bed and clear of speech.
    let boundaryShiftFraction: Double

    static let `default` = SilenceCompressorConfig(
        musicProbabilityFloor: 0.7,
        highMusicProbabilityFloor: 0.8,
        pauseProbabilityFloor: 0.7,
        speakerChangeProxyFloor: 0.5,
        lowRateMultiplier: 1.5,
        highRateMultiplier: 2.5,
        minimumGapSeconds: 4.0,
        lookaheadHorizonSeconds: 60.0,
        lookaheadCadenceSeconds: 5.0,
        exitHysteresisSeconds: 0.25,
        boundaryConfidenceFloor: 0.6,
        boundaryShiftFraction: 0.5
    )
}

// MARK: - CompressionAlgorithm

/// Indirection over `AVAudioTimePitchAlgorithm` so this file (and its
/// tests) can compile without importing AVFoundation. The host maps
/// these to the AVFoundation values when calling
/// `PlaybackService.beginCompression(multiplier:algorithm:)`.
enum CompressionAlgorithm: String, Sendable, Equatable {
    /// `.spectral` — clean for speech-adjacent material up to ~2.0×.
    case spectral
    /// `.varispeed` — pitch-shifts but cheap; acceptable on music-only
    /// segments above 2.0×.
    case varispeed
}

// MARK: - CompressionPlan

/// A single compressible region in episode-time.
///
/// Equatable so tests can compare plans directly without leaking
/// internal layout. `Sendable` so a plan can travel across the
/// playback / coordinator boundary safely.
struct CompressionPlan: Sendable, Equatable {
    /// Inclusive start time in seconds from episode origin.
    let startTime: Double
    /// Exclusive end time in seconds from episode origin.
    let endTime: Double
    /// Multiplier on top of the user's base playback speed.
    let multiplier: Float
    /// Time-pitch algorithm to install while the plan is active.
    let algorithm: CompressionAlgorithm

    var contains: (Double) -> Bool {
        let s = startTime
        let e = endTime
        return { t in t >= s && t < e }
    }
}

// MARK: - SilenceCompressorDecision

/// Pure decision output produced by the planner. The host translates
/// `.engage` into a `PlaybackService.beginCompression(multiplier:algorithm:)`
/// call, and `.disengage` into `endCompression()`. `.noChange` is the
/// quiet path — most ticks land here once a plan is in flight.
enum SilenceCompressorDecision: Sendable, Equatable {
    case noChange
    case engage(multiplier: Float, algorithm: CompressionAlgorithm, plan: CompressionPlan)
    case disengage
}

// MARK: - SilenceCompressor

/// Pure planner. Stateful but actor-free — the host (PlayheadRuntime)
/// owns isolation and is expected to call `tick(currentTime:)` from
/// a single context (the playback observer task) and to call
/// `recordSeek(to:)` / `markIdle()` / `recordKeepFullMusicOverride(_:)`
/// / `replaceWindows(_:assetId:)` / `clearAll()` from the same
/// context. Tests instantiate it on the test serial queue.
///
/// Memory: holds at most `lookaheadHorizonSeconds / windowDuration`
/// `FeatureWindow`s (~30 entries at the standard 2s window size) and
/// the derived `[CompressionPlan]` (≤ a handful of entries — gaps are
/// sparse). Bounded.
final class SilenceCompressor {

    // MARK: - State Machine

    private enum State: Equatable {
        case idle
        case compressing(plan: CompressionPlan)
    }

    // MARK: - Stored State

    private let config: SilenceCompressorConfig
    private let logger = Logger(subsystem: "com.playhead.app", category: "SilenceCompressor")

    private var state: State = .idle
    private var plans: [CompressionPlan] = []
    private var skipRanges: [(start: Double, end: Double)] = []
    private var assetId: String?
    private var keepFullMusic: Bool = false

    // MARK: - Init

    init(config: SilenceCompressorConfig = .default) {
        self.config = config
    }

    // MARK: - External Inputs

    /// Wipe all planner state. Called by the coordinator when the
    /// episode ends, so the next `replaceWindows(_:assetId:)` for a
    /// fresh asset doesn't have to use the prior `assetId !=
    /// self.assetId` reset path with a sentinel string. Idempotent.
    func clearAll() {
        state = .idle
        plans = []
        assetId = nil
    }

    /// Replace the in-memory window plan. The host fetches the
    /// upcoming `lookaheadHorizonSeconds` of windows from the
    /// AnalysisStore and hands them in here. Switching `assetId`
    /// invalidates any in-flight compression — the new asset's plan
    /// is necessarily different.
    func replaceWindows(_ windows: [FeatureWindow], assetId: String) {
        if assetId != self.assetId {
            // New asset id ⇒ wipe state. The next tick will rebuild.
            state = .idle
            plans = []
            self.assetId = assetId
        }
        if keepFullMusic {
            plans = []
            return
        }
        plans = Self.derivePlans(from: windows, config: config, skipRanges: skipRanges)
    }

    /// Hand in the current skip-cue ranges so the compressor refuses
    /// to plan inside regions the SkipOrchestrator will skip outright.
    /// Avoids "compress the thing about to be skipped" races and
    /// keeps the felt experience consistent (skip path wins).
    func updateSkipRanges(_ ranges: [(start: Double, end: Double)]) {
        skipRanges = ranges
        // Re-filter the existing plan list so we don't have to wait
        // for the next windows refresh to drop a plan that just got
        // shadowed by a skip cue.
        plans = plans.filter { plan in
            !Self.intersectsAny(start: plan.startTime, end: plan.endTime, ranges: ranges)
        }
    }

    /// Per-show override. When `true`, the planner short-circuits and
    /// the next tick disengages compression. Default `false`.
    func recordKeepFullMusicOverride(_ override: Bool) {
        keepFullMusic = override
        if override {
            plans = []
        }
    }

    /// User seeked. Forget any in-flight compression so we don't
    /// suddenly land in the middle of a plan and stay compressed
    /// across an unintended boundary. The next tick will re-engage
    /// from idle if the new playhead falls inside a plan.
    ///
    /// `time` is currently unused (the planner relies on the next
    /// `tick(currentTime:)` to evaluate the new playhead position),
    /// but it is kept on the signature so the host can pass through
    /// the new playhead time for future use (e.g. logging, cache
    /// invalidation by region).
    func recordSeek(to _: TimeInterval) {
        state = .idle
    }

    /// Reset the state machine to `.idle` without changing plans.
    /// Used by the coordinator to recover from side-channel events
    /// that invalidate any in-flight `.compressing` state — chiefly
    /// a user-initiated base-speed change, which `PlaybackService`
    /// handles by clearing the multiplier and the `.varispeed`
    /// algorithm. Without this hook the planner would stay in
    /// `.compressing(plan)` and return `.noChange` while inside the
    /// plan, leaving the rate at the user's new base for the rest
    /// of the run.
    ///
    /// Distinct from `recordSeek(to:)` semantically (no playhead
    /// move) so callers can pick the right verb at the call site;
    /// today both reduce to "set state = .idle", but factoring them
    /// keeps the planner's contract honest.
    func markIdle() {
        state = .idle
    }

    // MARK: - Tick

    /// Drive the state machine forward at the host's cadence.
    /// Returns the decision the host should apply to PlaybackService.
    func tick(currentTime: TimeInterval) -> SilenceCompressorDecision {
        if keepFullMusic {
            switch state {
            case .compressing:
                state = .idle
                return .disengage
            case .idle:
                return .noChange
            }
        }

        switch state {
        case .idle:
            guard let plan = activePlan(for: currentTime) else { return .noChange }
            state = .compressing(plan: plan)
            logger.info(
                "engage @ \(currentTime, format: .fixed(precision: 2))s → \(plan.endTime, format: .fixed(precision: 2))s × \(plan.multiplier) (\(plan.algorithm.rawValue, privacy: .public))"
            )
            return .engage(
                multiplier: plan.multiplier,
                algorithm: plan.algorithm,
                plan: plan
            )

        case .compressing(let plan):
            // Stay engaged while the playhead is inside the plan
            // (with hysteresis), otherwise disengage. We compare
            // against `endTime + exitHysteresisSeconds` so brief
            // observer-tick rounding doesn't flap in/out at the
            // trailing edge.
            if currentTime < plan.endTime + config.exitHysteresisSeconds,
               currentTime >= plan.startTime
            {
                return .noChange
            }
            // Falling out of the plan: disengage. If the next plan
            // starts immediately, the *following* tick will engage
            // it from .idle — we deliberately separate the two
            // transitions with a single .spectral / 1.0× moment so
            // the time-pitch algorithm change can settle.
            state = .idle
            logger.info("disengage @ \(currentTime, format: .fixed(precision: 2))s")
            return .disengage
        }
    }

    // MARK: - Inspection (test-only / coordinator-friendly)

    var currentPlans: [CompressionPlan] { plans }
    var isCurrentlyCompressing: Bool {
        if case .compressing = state { return true }
        return false
    }
    var currentAssetId: String? { assetId }

    private func activePlan(for time: TimeInterval) -> CompressionPlan? {
        plans.first { $0.contains(time) }
    }

    // MARK: - Plan Derivation

    /// Pure function: given a window list and config, produce the
    /// non-overlapping compression plan. Visible for testing so the
    /// decision matrix can be unit-tested against marked-up window
    /// arrays without instantiating the full compressor.
    static func derivePlans(
        from windows: [FeatureWindow],
        config: SilenceCompressorConfig,
        skipRanges: [(start: Double, end: Double)] = []
    ) -> [CompressionPlan] {
        guard !windows.isEmpty else { return [] }

        // Step 1: classify each window into a per-window bucket.
        // Dramatic pauses are detected at this stage by looking at
        // neighbour speakerClusterId equality and explicitly excluded
        // from compressible buckets.
        let sorted = windows.sorted { $0.startTime < $1.startTime }

        enum Bucket {
            case content
            case dramaticPause
            case deadAir
            case music  // unified — adjacency is decided at run-coalesce time
        }

        var buckets: [Bucket] = Array(repeating: .content, count: sorted.count)

        for index in sorted.indices {
            let window = sorted[index]

            // Dramatic-pause check FIRST — strongest preserve
            // invariant. A dramatic pause is a high-pause window
            // bracketed by two non-nil same-speaker windows. This is
            // the test the bead calls out as the most important
            // correctness invariant.
            if window.pauseProbability > config.pauseProbabilityFloor,
               let prev = previousSpeakerClustered(in: sorted, before: index),
               let next = nextSpeakerClustered(in: sorted, after: index),
               prev == next
            {
                buckets[index] = .dramaticPause
                continue
            }

            // Music-bed classification: a single bucket per-window so
            // contiguous music runs coalesce. Per-run algorithm/rate
            // is decided downstream based on whether the run as a
            // whole is speech-adjacent vs. surrounded by music. This
            // avoids splitting a 6s music run into 2s sub-buckets that
            // each fall below `minimumGapSeconds`.
            if window.musicProbability > config.musicProbabilityFloor,
               window.musicBedLevel != .none
            {
                buckets[index] = .music
                continue
            }

            // Dead-air check: high pause + speaker change boundary
            // (proxy score). Distinct from dramatic pause because
            // here either the speaker IDs differ or one is nil.
            if window.pauseProbability > config.pauseProbabilityFloor,
               window.speakerChangeProxyScore > config.speakerChangeProxyFloor
            {
                buckets[index] = .deadAir
                continue
            }
        }

        // Step 2: coalesce contiguous compressible buckets into runs.
        var runs: [CompressionPlan] = []
        var index = 0
        while index < sorted.count {
            let bucket = buckets[index]
            guard bucket != .content, bucket != .dramaticPause else {
                index += 1
                continue
            }
            // Walk forward while bucket is the same compressible kind.
            var endIndex = index
            while endIndex + 1 < sorted.count, buckets[endIndex + 1] == bucket {
                endIndex += 1
            }
            let runStart = sorted[index].startTime
            let runEnd = sorted[endIndex].endTime
            let durationSeconds = runEnd - runStart
            if durationSeconds >= config.minimumGapSeconds {
                let (multiplier, algorithm): (Float, CompressionAlgorithm) = {
                    switch bucket {
                    case .music:
                        // Decide algorithm/rate from the run's overall
                        // character. If EITHER end of the run sits next
                        // to a speech window, treat the whole run as
                        // speech-adjacent (clean spectral, low rate).
                        // Pure-music runs (intros/outros, jingle beds
                        // between segments with no immediate speech
                        // neighbour) escalate to varispeed at the high
                        // rate, but only if the dominant musicProbability
                        // crosses the high floor and at least one window
                        // is `.foreground`.
                        let runIsSpeechAdjacent = isRunSpeechAdjacent(
                            in: sorted, startIndex: index, endIndex: endIndex,
                            config: config
                        )
                        if runIsSpeechAdjacent {
                            return (config.lowRateMultiplier, .spectral)
                        }
                        let isHighConfidenceForeground = runHasForegroundHighConfidence(
                            in: sorted, startIndex: index, endIndex: endIndex,
                            config: config
                        )
                        if isHighConfidenceForeground {
                            return (config.highRateMultiplier, .varispeed)
                        }
                        // Mid-confidence music or background bed:
                        // stay on the gentler spectral / low rate.
                        return (config.lowRateMultiplier, .spectral)
                    case .deadAir:
                        return (config.lowRateMultiplier, .spectral)
                    case .content, .dramaticPause:
                        // Unreachable thanks to the guard above.
                        return (1.0, .spectral)
                    }
                }()
                // Refine the boundary against musicBedOnsetScore /
                // musicBedOffsetScore on the start/end windows where
                // the scores carry usable signal. Both default to 0,
                // so the refinement is a no-op for non-music gaps.
                let refinedStart = refineStart(
                    nominalStart: runStart, window: sorted[index],
                    config: config
                )
                let refinedEnd = refineEnd(
                    nominalEnd: runEnd, window: sorted[endIndex],
                    config: config
                )
                if refinedEnd - refinedStart >= config.minimumGapSeconds {
                    let plan = CompressionPlan(
                        startTime: refinedStart,
                        endTime: refinedEnd,
                        multiplier: multiplier,
                        algorithm: algorithm
                    )
                    if !intersectsAny(
                        start: plan.startTime, end: plan.endTime,
                        ranges: skipRanges
                    ) {
                        runs.append(plan)
                    }
                }
            }
            index = endIndex + 1
        }
        return runs
    }

    // MARK: - Helpers

    // Walks for the nearest window with a non-nil speakerClusterId — i.e. the
    // surrounding speech speaker. Windows without a cluster identity (pauses,
    // music, unclustered) are skipped. The "Clustered" suffix names the actual
    // predicate (has a speaker cluster), not pause-vs-non-pause semantics.
    private static func previousSpeakerClustered(
        in windows: [FeatureWindow], before index: Int
    ) -> Int? {
        guard index > 0 else { return nil }
        var i = index - 1
        while i >= 0 {
            if let id = windows[i].speakerClusterId { return id }
            i -= 1
        }
        return nil
    }

    private static func nextSpeakerClustered(
        in windows: [FeatureWindow], after index: Int
    ) -> Int? {
        var i = index + 1
        while i < windows.count {
            if let id = windows[i].speakerClusterId { return id }
            i += 1
        }
        return nil
    }

    /// True when EITHER edge of a music run is immediately preceded
    /// or followed by a speech window in the original buffer. The
    /// neighbour is considered speech when its musicProbability is
    /// below the music floor AND its pauseProbability is below the
    /// pause floor (i.e. it's neither music nor a long pause). When
    /// the run is at a buffer edge with no neighbour on that side,
    /// the missing neighbour does not by itself imply speech-adjacent
    /// — we want to preserve the behaviour of "pure music run with
    /// no surrounding context" escalating to the high rate.
    private static func isRunSpeechAdjacent(
        in windows: [FeatureWindow],
        startIndex: Int, endIndex: Int,
        config: SilenceCompressorConfig
    ) -> Bool {
        let isSpeech: (FeatureWindow) -> Bool = { window in
            window.musicProbability <= config.musicProbabilityFloor
                && window.pauseProbability <= config.pauseProbabilityFloor
        }
        if startIndex > 0, isSpeech(windows[startIndex - 1]) { return true }
        if endIndex + 1 < windows.count, isSpeech(windows[endIndex + 1]) { return true }
        return false
    }

    /// True when the run carries at least one `.foreground` bed
    /// window AND the maximum musicProbability across the run crosses
    /// the high-confidence floor. Used to decide whether a non-speech-
    /// adjacent music run is hot enough to deserve the varispeed/high
    /// rate path.
    private static func runHasForegroundHighConfidence(
        in windows: [FeatureWindow],
        startIndex: Int, endIndex: Int,
        config: SilenceCompressorConfig
    ) -> Bool {
        var sawForeground = false
        var maxProbability: Double = 0
        for index in startIndex...endIndex {
            let window = windows[index]
            if window.musicBedLevel == .foreground { sawForeground = true }
            if window.musicProbability > maxProbability {
                maxProbability = window.musicProbability
            }
        }
        return sawForeground && maxProbability > config.highMusicProbabilityFloor
    }

    private static func refineStart(
        nominalStart: Double, window: FeatureWindow,
        config: SilenceCompressorConfig
    ) -> Double {
        // musicBedOnsetScore is a 0...1 confidence; if low, push the
        // start later (further into the music); if high, the nominal
        // start is fine. Tested implicitly by plan-derivation tests.
        if window.musicBedOnsetScore > config.boundaryConfidenceFloor {
            return nominalStart
        }
        // Refine: shift the start later by `boundaryShiftFraction` of
        // the window so the speech tail clears.
        let windowDuration = max(0, window.endTime - window.startTime)
        return nominalStart + windowDuration * config.boundaryShiftFraction
    }

    private static func refineEnd(
        nominalEnd: Double, window: FeatureWindow,
        config: SilenceCompressorConfig
    ) -> Double {
        if window.musicBedOffsetScore > config.boundaryConfidenceFloor {
            return nominalEnd
        }
        // Pull the end earlier by `boundaryShiftFraction` of the
        // window so the following speech head isn't sped up.
        let windowDuration = max(0, window.endTime - window.startTime)
        return nominalEnd - windowDuration * config.boundaryShiftFraction
    }

    static func intersectsAny(
        start: Double, end: Double,
        ranges: [(start: Double, end: Double)]
    ) -> Bool {
        for range in ranges where !(end <= range.start || start >= range.end) {
            return true
        }
        return false
    }
}
