// SustainedMusicOffsetProposer.swift
// playhead-t1py: sustained-music-offset boundary PROPOSER.
//
// The doac post-roll (and the ~9–11 instrumental-outro shows like Morbid /
// SmartLess / PlanetMoney) is preceded by a long (15–27s) sustained tonal
// play-out that leads sharply into ad speech. That music→speech offset is the
// only deterministic cue for those post-rolls — but the pipeline REFINES
// (snaps an existing candidate), it never PROPOSES one, so an FM-missed post-
// roll produced no span at all. This proposer closes that gap: it scans for a
// long high-`musicProbability` run and PROPOSES a candidate span ending at the
// run's trailing music→speech edge, feeding candidate GENERATION in
// `RegionProposalBuilder` (the propose-not-refine seam, playhead-xtpf).
//
// Design:
//   • Pure `enum`, one `static func propose(...)` — no actor, no shared state,
//     no I/O. Sendable by construction.
//   • Thresholds on the already-persisted `FeatureWindow.musicProbability`
//     directly. The playhead-riiz composite (pauseFraction + sub-flux +
//     steadiness + tonalness + loudness, AUC 0.997) already bakes the
//     is-music-window discrimination into that field, so music-UNDER-speech
//     (speech pauses/flux break the predicate) scores low and does not fire —
//     no sub-frame re-derivation here.
//   • TARGETING signal only: the span is disposed `.markOnly` (banner) by
//     `DecisionMapper`, never a standalone auto-skip. A false music proposal
//     costs a banner, not a wrong skip — precision-safe by construction.

import Foundation

enum SustainedMusicOffsetProposer {

    /// GO_PORT_AS_IS thresholds (playhead-t1py). These are the ported detector
    /// constants from the scratchpad `musicoffset/` study — NOT fit on the gold
    /// (that is a later PR). Named so a future calibration pass has one place to
    /// tune.
    struct Config: Sendable {
        /// A window is "music" when its composite `musicProbability` reaches
        /// this. 0.76 is the riiz separation knee (measured music ≈ 0.776 vs
        /// speech ≈ 0.5); music-under-speech sits well below it.
        let musicRunThreshold: Double
        /// Minimum run span (seconds) to propose. 8s ≈ 4 of the 2s analysis
        /// windows — long enough to exclude a stray tonal syllable / jingle
        /// sting while catching the multi-second instrumental play-outs.
        let minRunSeconds: Double
        /// How many consecutive sub-threshold windows a run tolerates WITHOUT
        /// splitting (a single 2s dip in an otherwise-sustained swell should not
        /// break the run). The run ends only once the gap EXCEEDS this.
        let maxGapWindows: Int

        static let `default` = Config(
            musicRunThreshold: 0.76,
            minRunSeconds: 8.0,
            maxGapWindows: 1
        )
    }

    /// Scan `featureWindows` for maximal sustained-music runs and propose a
    /// candidate span `[runStart, trailingEdge)` for each run at least
    /// `config.minRunSeconds` long, where `trailingEdge` is the run's last music
    /// window's end (the music→speech boundary). `confidence` is the run
    /// strength: the mean `musicProbability` over the run's music windows.
    ///
    /// Deterministic: windows are sorted by `startTime` first, so identical
    /// input always yields identical output in span order.
    static func propose(
        featureWindows: [FeatureWindow],
        episodeDuration: Double,
        config: Config = .default
    ) -> [ProposedSpan] {
        guard !featureWindows.isEmpty else { return [] }

        let windows = featureWindows.sorted { $0.startTime < $1.startTime }

        var spans: [ProposedSpan] = []

        // Current run state.
        var runStartIndex: Int? = nil     // first music window of the open run
        var lastMusicIndex: Int? = nil    // last music window of the open run
        var musicProbSum = 0.0            // Σ musicProbability over run music windows
        var musicWindowCount = 0          // count of music windows in the run
        var gapCount = 0                  // consecutive sub-threshold windows since lastMusicIndex

        func closeRun() {
            defer {
                runStartIndex = nil
                lastMusicIndex = nil
                musicProbSum = 0.0
                musicWindowCount = 0
                gapCount = 0
            }
            guard let startIndex = runStartIndex,
                  let endIndex = lastMusicIndex,
                  musicWindowCount > 0 else { return }

            let runStart = windows[startIndex].startTime
            // Trailing edge = end of the last music window (the music→speech
            // offset). Clamp defensively to a known episode duration so a run
            // that reaches EOF cannot propose past the episode.
            var trailingEdge = windows[endIndex].endTime
            if episodeDuration > 0 {
                trailingEdge = min(trailingEdge, episodeDuration)
            }

            guard trailingEdge - runStart >= config.minRunSeconds else { return }

            spans.append(ProposedSpan(
                startTime: runStart,
                endTime: trailingEdge,
                confidence: musicProbSum / Double(musicWindowCount)
            ))
        }

        for index in windows.indices {
            let isMusic = windows[index].musicProbability >= config.musicRunThreshold
            if isMusic {
                if runStartIndex == nil { runStartIndex = index }
                lastMusicIndex = index
                musicProbSum += windows[index].musicProbability
                musicWindowCount += 1
                gapCount = 0
            } else if runStartIndex != nil {
                gapCount += 1
                if gapCount > config.maxGapWindows {
                    // The gap exceeded tolerance — the run ended at
                    // `lastMusicIndex`. Close it and start fresh.
                    closeRun()
                }
            }
        }

        // Close a run that extends to the final window (music → end of episode).
        closeRun()

        return spans
    }
}
