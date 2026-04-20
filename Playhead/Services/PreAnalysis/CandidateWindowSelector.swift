// CandidateWindowSelector.swift
// playhead-c3pi: pure-function selection of candidate ASR windows for the
// pre-analysis cascade + the thin cascade-coordinator that tracks the
// readiness anchor across seek events.
//
// The Phase 2 detection cascade prioritises ASR work in this order:
//
//   1. Chapter-marker sponsor/ad windows (publisher-declared positives)
//   2. The single playhead-proximal window:
//        - Unplayed episodes → first 20 minutes from episode start
//        - Resumed episodes  → next 15 minutes from `playbackAnchor`,
//          clamped to the episode end
//   3. Episode-wide backfill (handled by the existing AnalysisWorkScheduler
//      tier cascade T0/T1/T2 — outside this selector's responsibility)
//
// The bead spec mandates this is a pure function over its inputs so the
// ordering can be exhaustively tested without standing up the scheduler.
// Side-effecting wiring (priority bumps on existing AnalysisJobs,
// re-latch on user seeks) lives at the call-sites that consume this
// function's output.
//
// Window-selection magic numbers (20-min unplayed, 15-min resumed, 30-s
// re-latch threshold) are sourced from `PreAnalysisConfig` so a future
// per-cohort experiment can move them without touching this file.

import Foundation
import OSLog

/// A single candidate window the cascade will ASR before episode-wide
/// backfill resumes. Returned by `CandidateWindowSelector.select(...)` in
/// strict cascade order.
///
/// Equatable + Hashable so tests can compare entire selection arrays
/// without per-field assertions; Sendable because callers cross actor
/// boundaries to enqueue downstream work.
struct CandidateWindow: Sendable, Equatable, Hashable {
    /// Source of this window. Sponsor-chapter windows come first in the
    /// returned array (highest priority); the proximal window comes last.
    enum Kind: Sendable, Equatable, Hashable {
        /// A publisher-declared chapter marker classified as ad/sponsor by
        /// `ChapterEvidenceParser`. These are seeded ahead of the proximal
        /// window because the chapter is already "high-confidence
        /// positive" — only ASR is needed to feed the classifier.
        case sponsorChapter
        /// The playhead-proximal window — first 20 min unplayed or next
        /// 15 min from the readiness anchor for resumed episodes.
        case proximal
    }

    /// Time range (seconds from episode start) the window covers. Always
    /// a closed range with `lowerBound <= upperBound`.
    let range: ClosedRange<TimeInterval>
    /// Source classification (`.sponsorChapter` / `.proximal`).
    let kind: Kind
}

/// Stateless selector for candidate ASR windows. Lives as an enum
/// namespace because every entry-point is `static` — there is no
/// per-instance state worth holding.
enum CandidateWindowSelector {

    /// Select candidate windows in cascade-execution order.
    ///
    /// Output ordering (strict, deterministic):
    ///   1. All `.sponsorChapter` windows in **episode-time order**
    ///      (`startTime` ascending).
    ///   2. The single `.proximal` window (omitted if the anchor is at or
    ///      past the episode end).
    ///
    /// - Parameters:
    ///   - episodeDuration: Episode length in seconds. Pass `nil` when
    ///     unknown — the proximal window is then unbounded by episode
    ///     end (still bounded by the configured window length).
    ///   - playbackAnchor: Last-committed playhead position in seconds.
    ///     `nil` ⇒ unplayed (use `episodeStart = 0`).
    ///   - chapterEvidence: Parsed chapter markers (any disposition).
    ///     The selector filters to `.adBreak` chapters with a closed
    ///     time range; everything else is ignored.
    ///   - config: Source of named constants (window lengths). Tests
    ///     pass `PreAnalysisConfig()` so a default-config drift breaks
    ///     here too.
    /// - Returns: Cascade-ordered window list. Empty when there is no
    ///   work to do (e.g. anchor past episode end, no sponsor chapters).
    static func select(
        episodeDuration: TimeInterval?,
        playbackAnchor: TimeInterval?,
        chapterEvidence: [ChapterEvidence],
        config: PreAnalysisConfig
    ) -> [CandidateWindow] {
        var result: [CandidateWindow] = []

        // 1. Sponsor-chapter windows.
        let sponsorWindows = sponsorChapterWindows(
            chapterEvidence: chapterEvidence,
            episodeDuration: episodeDuration
        )
        result.append(contentsOf: sponsorWindows)

        // 2. Proximal window.
        if let proximal = proximalWindow(
            episodeDuration: episodeDuration,
            playbackAnchor: playbackAnchor,
            config: config
        ) {
            result.append(proximal)
        }

        return result
    }

    /// Seeded readiness anchor for `select(...)`'s `playbackAnchor`
    /// argument. Mirrors the spec:
    ///
    ///   - Unplayed episode (`playbackAnchor == nil`) → `0`
    ///     (`episodeStart`).
    ///   - Resumed episode → `playbackAnchor` (the last-committed
    ///     playhead from `Episode.playbackAnchor`).
    ///
    /// Exposed as a named helper so call-sites that need the canonical
    /// anchor for downstream surfaces (CoverageSummary derivation,
    /// telemetry) get the same answer the selector uses.
    static func readinessAnchor(playbackAnchor: TimeInterval?) -> TimeInterval {
        playbackAnchor ?? 0
    }

    /// Whether a seek from `previousAnchor` to `newPosition` should be
    /// treated as a re-latch event — i.e. the candidate-window selection
    /// should be re-computed against the new position.
    ///
    /// Spec rule: re-latch when the user seeks **more than 30 seconds**
    /// away from the prior anchor. The check is strictly greater than
    /// the threshold so a routine 30-s skip-forward is NOT treated as a
    /// re-latch.
    ///
    /// `previousAnchor == nil` ⇒ first-ever position update; always
    /// relatches.
    static func shouldRelatch(
        previousAnchor: TimeInterval?,
        newPosition: TimeInterval,
        threshold: TimeInterval
    ) -> Bool {
        guard let previousAnchor else { return true }
        return abs(newPosition - previousAnchor) > threshold
    }

    // MARK: - Private helpers

    /// Build sponsor-chapter windows from chapter evidence. Only
    /// `.adBreak` chapters with a closed `[startTime, endTime]` range
    /// inside the episode survive — open-ended sponsor chapters cannot
    /// be turned into a fixed ASR target without inventing a duration.
    /// Out-of-bounds chapters are dropped; partially-overflowing
    /// chapters are clamped to the episode end.
    private static func sponsorChapterWindows(
        chapterEvidence: [ChapterEvidence],
        episodeDuration: TimeInterval?
    ) -> [CandidateWindow] {
        let sponsors = chapterEvidence
            .filter { $0.disposition == .adBreak }
            .compactMap { evidence -> CandidateWindow? in
                guard let endTime = evidence.endTime else { return nil }
                guard endTime > evidence.startTime else { return nil }
                guard evidence.startTime >= 0 else { return nil }

                let lower = evidence.startTime
                let upper: TimeInterval
                if let duration = episodeDuration {
                    // Drop chapters that start beyond the episode end.
                    guard lower < duration else { return nil }
                    upper = min(endTime, duration)
                } else {
                    upper = endTime
                }
                guard upper > lower else { return nil }
                return CandidateWindow(range: lower...upper, kind: .sponsorChapter)
            }

        return sponsors.sorted { $0.range.lowerBound < $1.range.lowerBound }
    }

    /// Build the single playhead-proximal window from the anchor + config.
    /// Returns `nil` when there is no time left in the episode to cover
    /// (anchor at or past `episodeDuration`).
    private static func proximalWindow(
        episodeDuration: TimeInterval?,
        playbackAnchor: TimeInterval?,
        config: PreAnalysisConfig
    ) -> CandidateWindow? {
        let anchor = readinessAnchor(playbackAnchor: playbackAnchor)
        let windowLength: TimeInterval = (playbackAnchor == nil)
            ? config.unplayedCandidateWindowSeconds
            : config.resumedCandidateWindowSeconds

        let rawUpper = anchor + windowLength
        let upper: TimeInterval
        if let duration = episodeDuration {
            guard anchor < duration else { return nil }
            upper = min(rawUpper, duration)
        } else {
            upper = rawUpper
        }

        guard upper > anchor else { return nil }
        return CandidateWindow(range: anchor...upper, kind: .proximal)
    }
}

// MARK: - CandidateWindowCascade

/// Actor that tracks the current readiness anchor and the last-produced
/// candidate-window list per episode. Call-sites push events into the
/// cascade:
///
///   * `seed(episodeId:episodeDuration:playbackAnchor:chapterEvidence:)`
///     on download-start / re-enqueue — replaces the current latch and
///     returns the freshly-selected window order.
///   * `noteSeek(episodeId:newPosition:episodeDuration:chapterEvidence:)`
///     on every committed playhead update — when the delta exceeds
///     `seekRelatchThresholdSeconds`, the latch is rebased and the new
///     window order is returned. Otherwise returns `nil` (no re-latch).
///
/// The cascade is intentionally *advisory* in this bead: the windows it
/// emits are logged for SLI consumption and surfaced via `currentWindows`
/// so the scheduler (and tests) can assert on the cascade ordering
/// without the execution pipeline yet committing to per-slice jobs.
/// Per-slice execution and runner-side cascade consumption are tracked
/// by follow-up beads `playhead-xiz6` (runtime wiring),
/// `playhead-vhha` (seek-event wiring), and `playhead-swws` (runner
/// consumes cascade order + proves P90 ≤ 4h SLI).
actor CandidateWindowCascade {

    private let config: PreAnalysisConfig
    private let logger: Logger

    /// Per-episode current readiness anchor (last committed position /
    /// `episodeStart` for unplayed episodes). `nil` when the episode
    /// has never been seeded or noted.
    private var anchors: [String: TimeInterval?] = [:]

    /// Per-episode current ordered candidate windows (output of the most
    /// recent selection). Preserved across seek events below the
    /// re-latch threshold.
    private var windowsByEpisode: [String: [CandidateWindow]] = [:]

    /// playhead-swws: per-episode chapter evidence captured at `seed(...)`
    /// time so subsequent `noteSeek(...)` calls don't have to re-supply
    /// it. Without this cache, the persist-time relatch path
    /// (`PlayheadRuntime.noteCommittedPlayhead` →
    /// `AnalysisWorkScheduler.noteCommittedPlayhead`) would erase any
    /// sponsor-chapter windows the cascade selected at seed time —
    /// callers on the commit path don't carry chapter evidence in scope
    /// (the evidence comes from the metadata parse path).
    ///
    /// Stored alongside `anchors` so `forget(...)` clears both in lock
    /// step. Read by `noteSeek(...)` when the caller passes `nil` for
    /// the override; an explicit `.some([...])` override still wins so
    /// a future metadata-reparse path can refresh the evidence without
    /// re-seeding from scratch.
    private var chapterEvidenceByEpisode: [String: [ChapterEvidence]] = [:]

    init(
        config: PreAnalysisConfig = .load(),
        logger: Logger = Logger(subsystem: "com.playhead", category: "CandidateWindowCascade")
    ) {
        self.config = config
        self.logger = logger
    }

    /// Seed (or re-seed) the cascade for an episode. Typically called
    /// when the episode enters the scheduler queue — the call-site has
    /// the chapter evidence + metadata on hand and the cascade stores
    /// the latched anchor for subsequent seek notifications.
    ///
    /// - Returns: The ordered candidate windows the cascade now
    ///   associates with this episode.
    @discardableResult
    func seed(
        episodeId: String,
        episodeDuration: TimeInterval?,
        playbackAnchor: TimeInterval?,
        chapterEvidence: [ChapterEvidence]
    ) -> [CandidateWindow] {
        anchors[episodeId] = playbackAnchor
        chapterEvidenceByEpisode[episodeId] = chapterEvidence
        let windows = CandidateWindowSelector.select(
            episodeDuration: episodeDuration,
            playbackAnchor: playbackAnchor,
            chapterEvidence: chapterEvidence,
            config: config
        )
        windowsByEpisode[episodeId] = windows
        logger.info("Seeded cascade for episode=\(episodeId, privacy: .public) windows=\(windows.count) anchor=\(playbackAnchor ?? 0, privacy: .public)")
        return windows
    }

    /// Note a seek (or playhead commit). If the delta from the last
    /// latched anchor exceeds `seekRelatchThresholdSeconds`, the
    /// cascade re-bases and returns the new window order; otherwise
    /// returns `nil`.
    ///
    /// Callers that need the current list even when no relatch fired
    /// use `currentWindows(for:)`.
    ///
    /// playhead-swws: `chapterEvidence` is now optional. When the caller
    /// passes `nil`, the cascade reuses the evidence captured at the
    /// most recent `seed(...)` for this episode. This lets the
    /// commit-point caller (`PlayheadRuntime.noteCommittedPlayhead`)
    /// drop its `chapterEvidence: []` placeholder — sponsor-chapter
    /// windows now survive a re-latch instead of being erased on every
    /// seek. Callers that genuinely have fresh evidence (e.g. a
    /// metadata reparse) may pass `.some([...])` to override the cache.
    func noteSeek(
        episodeId: String,
        newPosition: TimeInterval,
        episodeDuration: TimeInterval?,
        chapterEvidence: [ChapterEvidence]? = nil
    ) -> [CandidateWindow]? {
        let previous = anchors[episodeId] ?? nil
        let relatch = CandidateWindowSelector.shouldRelatch(
            previousAnchor: previous,
            newPosition: newPosition,
            threshold: config.seekRelatchThresholdSeconds
        )
        guard relatch else { return nil }

        let evidence: [ChapterEvidence]
        if let override = chapterEvidence {
            evidence = override
            chapterEvidenceByEpisode[episodeId] = override
        } else {
            evidence = chapterEvidenceByEpisode[episodeId] ?? []
        }

        anchors[episodeId] = newPosition
        let windows = CandidateWindowSelector.select(
            episodeDuration: episodeDuration,
            playbackAnchor: newPosition,
            chapterEvidence: evidence,
            config: config
        )
        windowsByEpisode[episodeId] = windows
        logger.info("Re-latched cascade for episode=\(episodeId, privacy: .public) newAnchor=\(newPosition, privacy: .public) windows=\(windows.count)")
        return windows
    }

    /// playhead-swws: read-only accessor for the chapter evidence the
    /// cascade has cached for this episode. Returns `nil` when the
    /// episode has never been seeded; an empty array when the episode
    /// was seeded with no chapter evidence.
    func currentChapterEvidence(for episodeId: String) -> [ChapterEvidence]? {
        chapterEvidenceByEpisode[episodeId]
    }

    /// Current ordered windows for an episode, or `nil` if the cascade
    /// has never been seeded for it. Exposed for tests and for
    /// SLI emitters that need to report the planned order.
    func currentWindows(for episodeId: String) -> [CandidateWindow]? {
        windowsByEpisode[episodeId]
    }

    /// Current latched anchor for an episode. `.some(nil)` when the
    /// cascade was seeded with no playback anchor (unplayed), `nil`
    /// when the cascade has no record of the episode.
    func currentAnchor(for episodeId: String) -> TimeInterval?? {
        anchors[episodeId]
    }

    /// Forget an episode (call on deletion / download removal).
    func forget(episodeId: String) {
        anchors.removeValue(forKey: episodeId)
        windowsByEpisode.removeValue(forKey: episodeId)
        chapterEvidenceByEpisode.removeValue(forKey: episodeId)
    }
}
