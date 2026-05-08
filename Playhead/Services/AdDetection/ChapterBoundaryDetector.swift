// ChapterBoundaryDetector.swift
// playhead-au2v.1.4: Heuristic chapter boundary candidate generator.
//
// This is the "B1" stage of the inferred-chapter pipeline. It reads
// already-cached features from upstream services (FeatureExtractionService
// for music/RMS/pause windows, the speaker-clustering pipeline for cluster
// IDs, LexicalScanner for category hits) and emits a chronologically
// ordered list of `ChapterCandidate`s. Each candidate carries a per-boundary
// confidence and the set of signals that fired at that boundary.
//
// What it is NOT:
//   - It does not run any new acoustic / DSP work. All inputs are already
//     computed and cached upstream. The detector is a pure function on
//     a pre-built `ChapterFeatureSnapshot`.
//   - It does not apply density gates (rate caps, cap-and-merge,
//     pathological-rate abort). That is bead 5 (au2v.1.5).
//   - It does not label chapters with disposition / type / title.
//     That is bead 7 (au2v.1.7) via the FM labeler.
//   - It does not wire up the phase that produces the `ChapterPlan`
//     consumed by the rest of the pipeline. That is bead 12 (au2v.1.12).
//
// Adapter rationale (`ChapterFeatureSnapshot`):
//   The upstream feature shapes (`FeatureWindow`, `LexicalHit`, transcript
//   chunks) carry many fields the boundary detector does not need, and the
//   speaker-clustering pipeline produces cluster IDs separately from the
//   pure-acoustic `FeatureWindow.speakerClusterId` (which is currently
//   always nil in production extraction; see FeatureExtraction.swift line
//   ~772). Rather than coupling this detector to the wire shape of every
//   upstream service, we define a small input struct (`ChapterFeatureSnapshot`)
//   that the bead-12 phase wiring will populate from whichever sources it
//   has at hand. This keeps the detector unit-testable in isolation and
//   leaves the source-projection concern as an explicit out-of-scope
//   wiring step. The fields are deliberately closed over the four
//   spec-named heuristic signals — extending the detector with a fifth
//   signal will require both adding a field here and adding handling
//   logic below.
//
// Performance:
//   The detector is required to finish in <50ms on a 60-minute show. The
//   inputs scale at most linearly in episode duration (one music window
//   per ~2s, one speaker window per ASR chunk, etc.), so a 60-min show
//   has on the order of 1800 music windows, ~600-1000 speaker windows,
//   <100 lexical hits, ~1800 pause windows. All loops below are O(n)
//   over those arrays with no nested scans, no allocations in the hot
//   loop beyond a single output array, and no string work. A 50ms budget
//   is comfortable; a deterministic perf test asserts runtime against a
//   simulator-tolerant bound (200ms; simulators are 2-3x slower than
//   target hardware, see MinimalContiguousSpanDecoder perf test).

import Foundation

// MARK: - ChapterCandidate

/// A candidate chapter boundary emitted by `ChapterBoundaryDetector`.
///
/// `startTime` is the timestamp at which a chapter starts. The detector
/// always includes a synthetic candidate at `0.0` (every episode starts a
/// chapter at episode start). It does NOT include an episode-end boundary
/// — chapters are represented by start times and the last chapter
/// implicitly ends at the episode duration.
///
/// `boundaryConfidence` is in `[0, 1]` and is the normalized weighted sum
/// of the signal contributions at that boundary; see
/// `ChapterBoundaryDetector.signalWeights` for the per-signal weights.
struct ChapterCandidate: Sendable, Equatable {
    /// Boundary timestamp; the chapter starts here.
    let startTime: TimeInterval
    /// Boundary confidence in `[0, 1]`, normalized so a boundary with all
    /// four signals firing reaches `1.0`.
    let boundaryConfidence: Float
    /// Signals that contributed to this boundary; for diagnostics.
    /// May be empty for the synthetic t=0 boundary.
    let triggeringSignals: [BoundarySignal]
}

// MARK: - BoundarySignal

/// The four heuristic signals the boundary detector recognizes.
/// Documented per-signal weight is fixed in `ChapterBoundaryDetector.signalWeights`.
enum BoundarySignal: Sendable, Equatable, Hashable, CaseIterable {
    /// A music probability delta of more than `musicProbabilityDelta`
    /// between two adjacent music windows. Catches intros, outros,
    /// ad jingles.
    case musicTransition
    /// A speaker cluster ID change where the new cluster sustains for
    /// strictly more than `minSpeakerRunDuration`. Filters brief
    /// crosstalk (announcer cuts in for 2-3s, etc).
    case speakerShift
    /// The dominant lexical category in a `lexicalBinDuration`-wide
    /// window changes from the dominant category in the prior window.
    /// Catches normal-content → ad-cue density spikes and back.
    case lexicalCategoryJump
    /// A run of low-energy pause windows totaling strictly more than
    /// `minLongPauseDuration`. Often marks structural transitions.
    case longPause
}

// MARK: - ChapterFeatureSnapshot inputs

/// One music probability observation for a `~windowDuration` slice of
/// audio. Sourced from `FeatureWindow.musicProbability` upstream.
struct ChapterMusicWindow: Sendable, Equatable {
    let startTime: TimeInterval
    let endTime: TimeInterval
    /// Music probability in `[0, 1]`. Values outside this range are
    /// clamped at consumption time.
    let musicProbability: Double
}

/// One speaker cluster observation. Sourced from the speaker-clustering
/// pipeline. `clusterId` is nullable because diarization may not be run
/// for every segment (e.g. fast-pass chunks); a nil cluster ID is treated
/// as "no signal" — neither a shift nor a continuation.
struct ChapterSpeakerWindow: Sendable, Equatable {
    let startTime: TimeInterval
    let endTime: TimeInterval
    let clusterId: Int?
}

/// One lexical hit summary, scoped to the fields the boundary detector
/// uses. Full `LexicalHit` carries a matched-text payload that we never
/// need here, so we drop it to keep the snapshot cheap to construct.
struct ChapterLexicalHit: Sendable, Equatable {
    let startTime: TimeInterval
    let category: LexicalPatternCategory
}

/// One pause/silence observation. Sourced from `FeatureWindow.pauseProbability`
/// or VAD anchors. `pauseProbability` >= `pauseThreshold` causes the
/// window to be treated as silence; contiguous silence runs >= the
/// long-pause duration trigger the long-pause signal.
struct ChapterPauseWindow: Sendable, Equatable {
    let startTime: TimeInterval
    let endTime: TimeInterval
    let pauseProbability: Double
}

/// The full input snapshot for a single boundary-detection pass.
///
/// All time fields are episode-local seconds (matching `FeatureWindow`,
/// `TranscriptChunk`, `LexicalHit`, etc.). All four input arrays are
/// allowed to be empty; the detector still returns at least the
/// synthetic t=0 boundary.
///
/// Input ordering contract:
///   - `musicWindows`, `speakerWindows`, and `pauseWindows` MUST be in
///     non-decreasing `startTime` order. Their detectors walk the
///     arrays as time-series and rely on contiguous adjacency to
///     compute deltas / sustained runs / pause durations. Out-of-order
///     elements would silently produce wrong output.
///   - `lexicalHits` may be in any order; the detector sorts hits
///     internally and filters non-finite / negative timestamps because
///     the hit producers (`LexicalScanner`) emit per-chunk and a
///     consumer may concatenate hits from chunks processed out of
///     order.
struct ChapterFeatureSnapshot: Sendable, Equatable {
    /// Episode duration in seconds. Used to clamp boundaries and avoid
    /// emitting candidates past the end of the episode. A duration of 0
    /// or less degenerates to "t=0 only" output.
    let episodeDuration: TimeInterval
    /// Music probability per ~2s window. MUST be sorted by
    /// `startTime` ascending; see snapshot-level docstring.
    let musicWindows: [ChapterMusicWindow]
    /// Speaker cluster windows. MUST be sorted by `startTime`
    /// ascending. Need not be the same cadence as `musicWindows`; the
    /// detector treats them independently.
    let speakerWindows: [ChapterSpeakerWindow]
    /// Lexical hits across the episode. May be in any order; the
    /// detector sorts and sanity-filters internally.
    let lexicalHits: [ChapterLexicalHit]
    /// Pause/silence per-window observations. MUST be sorted by
    /// `startTime` ascending.
    let pauseWindows: [ChapterPauseWindow]

    init(
        episodeDuration: TimeInterval,
        musicWindows: [ChapterMusicWindow] = [],
        speakerWindows: [ChapterSpeakerWindow] = [],
        lexicalHits: [ChapterLexicalHit] = [],
        pauseWindows: [ChapterPauseWindow] = []
    ) {
        self.episodeDuration = episodeDuration
        self.musicWindows = musicWindows
        self.speakerWindows = speakerWindows
        self.lexicalHits = lexicalHits
        self.pauseWindows = pauseWindows
    }
}

// MARK: - Configuration

/// Per-detector tuning. The default is what the bead spec calls for; a
/// non-default config exists primarily for unit tests that want to
/// exercise a knob in isolation without reaching into `private`.
struct ChapterBoundaryDetectorConfig: Sendable, Equatable {

    // MARK: Signal weights

    /// Music onset/offset weight. Strong signal — music transitions
    /// reliably mark intros, outros, ad jingles. Highest weight.
    let musicTransitionWeight: Float
    /// Speaker shift weight. A real shift (one that holds for >5s)
    /// reliably marks a structural transition — guest swap, host return,
    /// ad voiceover entry. Second-highest weight.
    let speakerShiftWeight: Float
    /// Lexical category jump weight. Useful but noisier — category
    /// distributions are sparse on short windows and a single hit can
    /// be a false positive.
    let lexicalCategoryJumpWeight: Float
    /// Long pause weight. Lowest — pauses between segments are common
    /// and many of them are not chapter boundaries. Treated as a
    /// supporting signal, not a primary trigger.
    let longPauseWeight: Float

    // MARK: Music transition

    /// Minimum probability delta between adjacent music windows that
    /// counts as a transition. The bead spec calls out 0.5.
    let musicProbabilityDelta: Double

    // MARK: Speaker shift

    /// Minimum duration the new cluster must sustain after a shift for
    /// the shift to be counted. The bead spec calls out 5s (strict).
    /// At-or-below this duration the apparent shift is treated as
    /// crosstalk noise.
    let minSpeakerRunDuration: TimeInterval

    // MARK: Lexical category

    /// Width of the lexical-category density bin, in seconds. The
    /// dominant category in each bin is compared to the dominant
    /// category in the prior bin; a change emits a boundary at the
    /// start of the new bin. 30s matches `LexicalScannerConfig.default
    /// .mergeGapThreshold`.
    let lexicalBinDuration: TimeInterval

    // MARK: Long pause

    /// `pauseProbability` >= this threshold marks a window as silence.
    /// Contiguous silence runs are summed to detect long pauses.
    /// 0.5 is the natural midpoint of the [0,1] probability.
    let pauseThreshold: Double
    /// Minimum cumulative silence duration that triggers `longPause`.
    /// The bead spec calls out 2s (strict). Runs of duration at or
    /// below this value do not emit.
    let minLongPauseDuration: TimeInterval

    // MARK: Output shaping

    /// Minimum confidence required for a non-zero boundary to be emitted.
    /// The synthetic t=0 boundary always emits regardless of this gate.
    /// A boundary fires through this gate only if the weighted sum of
    /// firing-signal weights is at least this much; with the default
    /// weights, a single signal strength `0.4` (music) >= `0.10` is fine,
    /// but a single weight-0.10 signal (longPause) alone is below it.
    let minBoundaryConfidence: Float
    /// Minimum spacing between consecutive emitted boundaries. Adjacent
    /// signal events within this window are coalesced onto the
    /// strongest-confidence boundary in the cluster.
    let minBoundarySpacing: TimeInterval

    static let `default` = ChapterBoundaryDetectorConfig(
        // Weights sum to 1.0 so a boundary at which all four signals
        // fire produces `boundaryConfidence == 1.0` exactly. The 0.4 /
        // 0.3 / 0.2 / 0.1 split comes from the bead spec; rationale:
        //   * music transitions are the most reliable single signal
        //     (intros/outros/ad jingles produce clean deltas), so they
        //     dominate;
        //   * a sustained speaker shift is the next-most-reliable
        //     non-acoustic signal — host swaps and announcer entries
        //     are the bread and butter of chapter boundaries on
        //     interview podcasts;
        //   * lexical category jumps are useful but noisy on short
        //     windows;
        //   * long pauses alone are common false-positive territory
        //     (breath gaps, edit cuts) and are kept low so they only
        //     contribute when stacked with another signal.
        musicTransitionWeight: 0.4,
        speakerShiftWeight: 0.3,
        lexicalCategoryJumpWeight: 0.2,
        longPauseWeight: 0.1,

        musicProbabilityDelta: 0.5,
        minSpeakerRunDuration: 5.0,
        lexicalBinDuration: 30.0,
        pauseThreshold: 0.5,
        minLongPauseDuration: 2.0,

        // 0.10 = the smallest single weight (longPause). The gate is
        // inclusive (`>=`), so longPause alone is exactly on the
        // threshold and DOES emit; the other three signals all clear
        // the gate on their own. Stacked combinations always clear
        // the gate. Setting this gate above 0.10 (e.g. 0.11) would
        // suppress the longPause signal entirely.
        minBoundaryConfidence: 0.10,
        // 1.0s prevents two adjacent music-transition windows (which
        // describe the same intro/outro on a 2s grid) from emitting
        // two separate boundaries 2s apart. Coarse enough to merge
        // chunked signal events, fine enough that the t=0 boundary
        // does not absorb an ad break that legitimately starts a few
        // seconds in.
        minBoundarySpacing: 1.0
    )
}

// MARK: - ChapterBoundaryDetector

/// Heuristic chapter boundary detector. Pure function over a feature
/// snapshot; safe to call from any actor.
struct ChapterBoundaryDetector: Sendable {

    private let config: ChapterBoundaryDetectorConfig

    init(config: ChapterBoundaryDetectorConfig = .default) {
        self.config = config
    }

    // MARK: - Public API

    /// Detect chapter boundary candidates.
    ///
    /// Output guarantees:
    ///   - The first element is always a synthetic t=0 candidate.
    ///   - Boundaries are returned in chronological order.
    ///   - No two boundaries are closer than `config.minBoundarySpacing`.
    ///   - All boundary times are within `[0, episodeDuration]`.
    ///   - `boundaryConfidence` is in `[0, 1]`.
    func detect(features: ChapterFeatureSnapshot) -> [ChapterCandidate] {
        // Always include the synthetic episode-start boundary.
        // Confidence 1.0 — the first chapter always exists.
        let startBoundary = ChapterCandidate(
            startTime: 0,
            boundaryConfidence: 1.0,
            triggeringSignals: []
        )

        // Degenerate input: no episode duration to scan over.
        guard features.episodeDuration > 0 else {
            return [startBoundary]
        }

        // Collect raw signal events from each detector. Each event is
        // (time, signal, weight). All four detectors are O(n) in their
        // respective input arrays.
        var signalEvents: [SignalEvent] = []
        signalEvents.append(contentsOf: detectMusicTransitions(features.musicWindows))
        signalEvents.append(contentsOf: detectSpeakerShifts(features.speakerWindows))
        signalEvents.append(contentsOf: detectLexicalCategoryJumps(features.lexicalHits))
        signalEvents.append(contentsOf: detectLongPauses(features.pauseWindows))

        guard !signalEvents.isEmpty else {
            return [startBoundary]
        }

        // Sort events by time, then cluster within
        // `minBoundarySpacing`. Each cluster becomes one boundary; its
        // `boundaryConfidence` is the sum of the firing signals'
        // weights (each signal counted at most once per cluster), and
        // its `triggeringSignals` is the deduplicated set of firing
        // signals.
        //
        // Sort uses a deterministic secondary key (the signal's
        // CaseIterable position) so two events at the same time always
        // hit the loop in the same order across runs. Without this,
        // Swift's sort is not stable and identical inputs could
        // produce different `clusterEarliestEventTime` values, which
        // would feed back into `triggeringSignals` ordering downstream.
        signalEvents.sort { lhs, rhs in
            if lhs.time != rhs.time { return lhs.time < rhs.time }
            return lhs.signalSortIndex < rhs.signalSortIndex
        }

        var rawBoundaries: [ChapterCandidate] = []
        rawBoundaries.reserveCapacity(signalEvents.count)

        var clusterTime: TimeInterval = signalEvents[0].time
        var clusterSignals: Set<BoundarySignal> = []
        var clusterEarliestEventTime: TimeInterval = signalEvents[0].time

        for event in signalEvents {
            if event.time - clusterTime <= config.minBoundarySpacing {
                clusterSignals.insert(event.signal)
                // Use the latest event in the cluster as the cluster's
                // moving anchor so a long, slow drift of events still
                // groups correctly. Time-of-emission stays anchored to
                // the earliest event so the boundary lands at the first
                // hint of the transition.
                clusterTime = event.time
            } else {
                rawBoundaries.append(makeCandidate(
                    time: clusterEarliestEventTime,
                    signals: clusterSignals,
                    duration: features.episodeDuration
                ))
                clusterTime = event.time
                clusterEarliestEventTime = event.time
                clusterSignals = [event.signal]
            }
        }
        // Flush trailing cluster.
        rawBoundaries.append(makeCandidate(
            time: clusterEarliestEventTime,
            signals: clusterSignals,
            duration: features.episodeDuration
        ))

        // Filter by min-confidence gate, then drop any boundary that
        // landed on or before the synthetic t=0 (within
        // minBoundarySpacing) since the synthetic boundary already
        // covers it. Also clamp upper bound by episodeDuration. Already
        // done in `makeCandidate`; here we just enforce the gate and
        // synthetic-boundary dedup.
        let filtered = rawBoundaries.filter { candidate in
            candidate.boundaryConfidence >= config.minBoundaryConfidence
                && candidate.startTime > config.minBoundarySpacing
                && candidate.startTime < features.episodeDuration
        }

        return [startBoundary] + filtered
    }

    // MARK: - Signal: music transitions

    /// Detect music probability deltas > `musicProbabilityDelta` between
    /// adjacent windows. Emits an event at the START time of the second
    /// window — that is the timestamp at which the transition has
    /// completed and a new chapter (with or without music) begins.
    private func detectMusicTransitions(
        _ windows: [ChapterMusicWindow]
    ) -> [SignalEvent] {
        guard windows.count >= 2 else { return [] }

        var out: [SignalEvent] = []
        out.reserveCapacity(windows.count / 4)

        for index in 1..<windows.count {
            let prev = clampProbability(windows[index - 1].musicProbability)
            let curr = clampProbability(windows[index].musicProbability)
            let delta = abs(curr - prev)
            if delta > config.musicProbabilityDelta {
                out.append(SignalEvent(
                    time: windows[index].startTime,
                    signal: .musicTransition,
                    weight: config.musicTransitionWeight
                ))
            }
        }
        return out
    }

    // MARK: - Signal: speaker shifts

    /// Detect cluster ID transitions where the new cluster sustains
    /// for strictly more than `minSpeakerRunDuration`. Emits the event
    /// at the start of the first window with the new cluster.
    ///
    /// Implementation: walk the speaker windows tracking the active
    /// cluster ID. On every change to a non-nil cluster ID, look ahead
    /// to compute the contiguous run length of the new cluster and
    /// emit only if it lasts strictly longer than the minimum. Nil
    /// cluster IDs are skipped during the lookahead — they do not add
    /// to the run, but they also do not break it (otherwise chunked
    /// diarization with sub-second gaps would never qualify). The run
    /// breaks on the first non-nil cluster window with a different
    /// cluster ID. Run duration is measured from the start of the
    /// first matching window to the end of the last matching window
    /// (gaps between matching windows count toward the duration so
    /// that a 4s + 1s-nil-gap + 4s pattern measures 9s sustained, not
    /// 8s).
    ///
    /// Complexity: in the common case of clusters that persist for
    /// many windows the loop is O(N) — sustained windows are skipped
    /// via the `establishedCluster == candidateCluster` early-out and
    /// each lookahead amortizes against many subsequent skip
    /// iterations. Pathological adversarial input (e.g. every
    /// adjacent window a different cluster, with no run sustaining)
    /// degrades to O(N²) because each non-sustained candidate redoes
    /// its own bounded lookahead. Real diarization output never looks
    /// like that — production runs persist on the order of tens of
    /// seconds — and the perf test (1800-window 60-min show with
    /// cluster toggling every ~30s) confirms <50ms wall time on
    /// device. If a future input distribution invalidates this
    /// assumption, hoist the lookahead state out of the per-window
    /// loop.
    private func detectSpeakerShifts(
        _ windows: [ChapterSpeakerWindow]
    ) -> [SignalEvent] {
        guard !windows.isEmpty else { return [] }

        var out: [SignalEvent] = []
        // The "established" cluster: the cluster ID we believe is
        // currently speaking, set only when we have observed a run
        // long enough to be confident (or when seeding the very first
        // cluster of the episode). Brief candidate clusters that fail
        // the >minSpeakerRunDuration gate do NOT advance this state —
        // otherwise a 2s announcer interruption inside a 30s monologue
        // would fabricate a spurious "shift back to host" event when
        // the host resumes.
        var establishedCluster: Int? = nil

        for index in 0..<windows.count {
            guard let candidateCluster = windows[index].clusterId else {
                continue
            }
            if establishedCluster == candidateCluster {
                continue
            }
            // Compute the sustained run length starting at `index`. We
            // skip nil-cluster windows during this lookahead — they do
            // not add to the run, but they also do not break it
            // (chunked diarization may have sub-second nil gaps inside
            // a real sustained run). The run breaks on the first
            // non-nil cluster window with a different cluster ID.
            var runEnd = windows[index].endTime
            var lookahead = index + 1
            while lookahead < windows.count {
                let nextWindow = windows[lookahead]
                if let nextCluster = nextWindow.clusterId {
                    if nextCluster != candidateCluster { break }
                    runEnd = nextWindow.endTime
                }
                lookahead += 1
            }
            let runDuration = runEnd - windows[index].startTime
            // Strict greater-than matches the bead spec wording
            // ("speaker cluster ID transition that lasts >5s"). A
            // shift sustained for exactly the minimum is treated as
            // crosstalk noise.
            let isSustained = runDuration > config.minSpeakerRunDuration
            if isSustained && establishedCluster != nil {
                out.append(SignalEvent(
                    time: windows[index].startTime,
                    signal: .speakerShift,
                    weight: config.speakerShiftWeight
                ))
            }
            // Advance establishedCluster only when the candidate
            // sustains. Two cases:
            //   1. First cluster of the episode (establishedCluster
            //      nil): seed it on first sustained run so subsequent
            //      shifts have a baseline to compare against.
            //   2. Real shift (establishedCluster non-nil and
            //      isSustained): update because we just emitted.
            // Filtered (non-sustained) candidates leave the prior
            // cluster intact; the next non-nil cluster window will be
            // re-evaluated against it.
            if isSustained {
                establishedCluster = candidateCluster
            }
        }
        return out
    }

    // MARK: - Signal: lexical category jumps

    /// Bin lexical hits into `lexicalBinDuration`-wide windows; for
    /// each bin compute the dominant category (most-frequent), and
    /// emit an event whenever the just-closed bin's dominant differs
    /// from the dominant of the most-recent prior non-empty bin. The
    /// event lands at the START of the just-closed bin.
    ///
    /// Empty bins (bins with zero hits) are simply skipped — they
    /// neither reset the prior-dominant tracker nor emit an event.
    /// Two non-empty bins separated by any number of empty bins
    /// behave as if they were adjacent for jump-detection purposes.
    ///
    /// Implementation note on event time: the algorithm iterates
    /// hits in time-sorted order. When a hit's bin index differs
    /// from `currentBinIndex` we close out `currentBinIndex` — its
    /// dominant gets compared to `lastDominant` and the event (if
    /// any) is emitted BEFORE we advance `currentBinIndex` to the
    /// new bin. Thus `time: TimeInterval(currentBinIndex) *
    /// binDuration` is the start of the bin whose dominant just got
    /// determined, i.e. the "jump-into" timestamp.
    private func detectLexicalCategoryJumps(
        _ hits: [ChapterLexicalHit]
    ) -> [SignalEvent] {
        guard hits.count >= 2 else { return [] }

        let binDuration = config.lexicalBinDuration
        guard binDuration > 0 else { return [] }

        // Sort by time defensively. The detector contract says inputs
        // can be in any order; test cases may not pre-sort. Drop
        // hits with non-finite or negative timestamps — those would
        // otherwise produce ill-formed bin indices (negative bin
        // indices truncate toward zero, which silently merges
        // pre-episode hits into bin 0). Real upstream producers
        // should never emit those, but the detector should be
        // robust to bad inputs.
        let sortedHits = hits
            .filter { $0.startTime.isFinite && $0.startTime >= 0 }
            .sorted { $0.startTime < $1.startTime }
        guard sortedHits.count >= 2 else { return [] }

        var out: [SignalEvent] = []
        var lastDominant: LexicalPatternCategory? = nil
        var currentBinIndex = Int(sortedHits[0].startTime / binDuration)
        var binCounts: [LexicalPatternCategory: Int] = [:]

        for hit in sortedHits {
            let hitBinIndex = Int(hit.startTime / binDuration)
            if hitBinIndex != currentBinIndex {
                // Close out the previous bin.
                if let dominant = dominantCategory(in: binCounts),
                   dominant != lastDominant {
                    if lastDominant != nil {
                        out.append(SignalEvent(
                            time: TimeInterval(currentBinIndex) * binDuration,
                            signal: .lexicalCategoryJump,
                            weight: config.lexicalCategoryJumpWeight
                        ))
                    }
                    lastDominant = dominant
                }
                binCounts.removeAll(keepingCapacity: true)
                currentBinIndex = hitBinIndex
            }
            binCounts[hit.category, default: 0] += 1
        }
        // Flush the final bin.
        if let dominant = dominantCategory(in: binCounts),
           dominant != lastDominant,
           lastDominant != nil {
            out.append(SignalEvent(
                time: TimeInterval(currentBinIndex) * binDuration,
                signal: .lexicalCategoryJump,
                weight: config.lexicalCategoryJumpWeight
            ))
        }

        return out
    }

    /// Return the most-frequent category in `counts`. Ties are broken
    /// by `LexicalPatternCategory` raw value so the output is
    /// deterministic across hash-seed runs.
    private func dominantCategory(
        in counts: [LexicalPatternCategory: Int]
    ) -> LexicalPatternCategory? {
        guard !counts.isEmpty else { return nil }
        return counts.max { lhs, rhs in
            if lhs.value != rhs.value { return lhs.value < rhs.value }
            return lhs.key.rawValue > rhs.key.rawValue
        }?.key
    }

    // MARK: - Signal: long pauses

    /// Detect contiguous runs of pause windows (pauseProbability >=
    /// `pauseThreshold`) totaling strictly more than
    /// `minLongPauseDuration`. Emits one event per qualifying run, at
    /// the start of the run.
    ///
    /// "Contiguous" is defined as adjacent windows in array order whose
    /// `pauseProbability` clears the threshold. We do not require the
    /// pause windows to be temporally contiguous (no time-gap check)
    /// because upstream feature extraction emits one window per audio
    /// slice and a gap in pause-window coverage is itself a signal —
    /// usually it just means a non-pause window was interleaved, in
    /// which case the run already broke.
    private func detectLongPauses(
        _ windows: [ChapterPauseWindow]
    ) -> [SignalEvent] {
        guard !windows.isEmpty else { return [] }

        var out: [SignalEvent] = []

        var runStart: TimeInterval? = nil
        var runEnd: TimeInterval? = nil
        for window in windows {
            if window.pauseProbability >= config.pauseThreshold {
                if runStart == nil {
                    runStart = window.startTime
                }
                runEnd = window.endTime
            } else {
                if let start = runStart, let end = runEnd, end - start > config.minLongPauseDuration {
                    out.append(SignalEvent(
                        time: start,
                        signal: .longPause,
                        weight: config.longPauseWeight
                    ))
                }
                runStart = nil
                runEnd = nil
            }
        }
        // Flush trailing run. Uses strict-greater to match the
        // mid-loop branch above and the docstring contract — a pause
        // run of exactly `minLongPauseDuration` does NOT emit, whether
        // the run is followed by speech or simply ends the input. (R3
        // fix: prior `>=` here meant a trailing 2s pause silently
        // emitted while a non-trailing 2s pause did not.)
        if let start = runStart, let end = runEnd, end - start > config.minLongPauseDuration {
            out.append(SignalEvent(
                time: start,
                signal: .longPause,
                weight: config.longPauseWeight
            ))
        }

        return out
    }

    // MARK: - Helpers

    /// Build a `ChapterCandidate` from a clustered set of signals.
    /// Confidence is the sum of the signals' weights (each signal
    /// contributes at most once per cluster — a music event at t=10 and
    /// another at t=10.4 both fall in one cluster and produce a single
    /// 0.4-weight contribution, not 0.8). Output is clamped to `[0, 1]`
    /// in case a non-default config has weights summing above 1.0.
    private func makeCandidate(
        time: TimeInterval,
        signals: Set<BoundarySignal>,
        duration: TimeInterval
    ) -> ChapterCandidate {
        // Accumulate in Double then narrow once at the end. Float
        // accumulation of 0.4 + 0.3 + 0.2 + 0.1 produces 0.99999994,
        // which when clamped to [0,1] reads as <1.0 and surprises
        // downstream consumers that expect "all four signals fire =
        // exactly 1.0". Doing the sum in Double avoids the drift,
        // and the narrowing cast at the end is exact for values in
        // [0, 1].
        var sum: Double = 0
        var orderedSignals: [BoundarySignal] = []
        // Iterate in canonical CaseIterable order so `triggeringSignals`
        // is deterministic across runs (Set iteration is not).
        for signal in BoundarySignal.allCases where signals.contains(signal) {
            sum += Double(weight(for: signal))
            orderedSignals.append(signal)
        }
        let clampedTime = max(0, min(time, duration))
        let clampedConfidence = Float(max(0, min(1, sum)))
        return ChapterCandidate(
            startTime: clampedTime,
            boundaryConfidence: clampedConfidence,
            triggeringSignals: orderedSignals
        )
    }

    private func weight(for signal: BoundarySignal) -> Float {
        switch signal {
        case .musicTransition:      return config.musicTransitionWeight
        case .speakerShift:         return config.speakerShiftWeight
        case .lexicalCategoryJump:  return config.lexicalCategoryJumpWeight
        case .longPause:            return config.longPauseWeight
        }
    }

    private func clampProbability(_ value: Double) -> Double {
        max(0, min(1, value))
    }

    // MARK: - Internal types

    private struct SignalEvent {
        let time: TimeInterval
        let signal: BoundarySignal
        let weight: Float

        /// CaseIterable index of `signal`, used as a deterministic
        /// secondary sort key when two events share a time. Computed
        /// on each access; `BoundarySignal.allCases` has only 4
        /// entries so `firstIndex(of:)` is effectively O(1) per call.
        /// The sort itself invokes this property O(N log N) times for
        /// N events; on a 60-min episode (low thousands of events)
        /// this is well within the 50ms detector budget. If the
        /// signal-set ever grows beyond a handful of cases, cache
        /// this value into a stored property at SignalEvent
        /// construction time.
        var signalSortIndex: Int {
            BoundarySignal.allCases.firstIndex(of: signal) ?? 0
        }
    }
}
