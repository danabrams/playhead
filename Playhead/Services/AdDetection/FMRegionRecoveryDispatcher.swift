// FMRegionRecoveryDispatcher.swift
// playhead-r2vz (PR2): FM recovery pass for the music-offset lexical gate.
//
// PR1 (`MusicOffsetLexicalGate`) suppresses UNCORROBORATED music-only
// proposals whose onset window carries no third-party ad-cue — a precision
// win that also drops ~42% of the real gold-miss ads whose reads are
// CUE-LESS / creative (Duffer cross-promo, FFRF underwriting). Lexical
// structurally cannot catch those (per Dan: lexical = attention, FM
// disposes — [[feedback_lexical_as_attention]]).
//
// This dispatcher gives exactly the gate-suppressed set ONE targeted FM
// look: it segments the region's music tail PLUS the post-edge onset window
// the gate inspected (where a cue-less creative ad's copy actually sits —
// see `regionSegments`), reuses the champion `.classification` coarse prompt,
// calls `respondCoarse`, and maps the resulting `CoarseDisposition` to an
// admit/drop verdict. The verdict is a
// GATE ONLY — the recovery pass never stamps `.foundationModel` origin or
// FM evidence onto a restored region, so `DecisionMapper.isMusicOnlyProvenance`
// still holds and a restored region can only ever decode to `.markOnly`
// (banner), never auto-skip. That markOnly-by-omission guarantee is
// correctness-critical (see `RegionShadowPhase.run`).
//
// Design grain mirrors `LiveShadowFMDispatcher`: the actual FM call is
// serialized through actor isolation (concurrency = 1) and creates a fresh
// session per region via `Runtime.makeSession()` + prewarm. Keeping the FM
// work inside this dispatcher (behind an injected `@Sendable` closure box)
// lets `RegionShadowPhase` stay a pure static enum — no new actor surface.

import Foundation
import OSLog

// MARK: - FMRegionVerdict

/// The verdict an FM recovery pass returns for a gate-suppressed music-only
/// region.
///
///   - `.ad`          → re-admit the region (restores its markOnly banner).
///   - `.content`     → leave it suppressed (= PR1 drop behavior).
///   - `.unavailable` → leave it suppressed (graceful degrade: FM refused,
///                       threw, abstained, or the runtime is unavailable).
enum FMRegionVerdict: Sendable, Equatable {
    case ad
    case content
    case unavailable
}

// MARK: - FMRegionRecoveryClassifier

/// A small `Sendable` box holding the injected recovery closure that
/// `RegionShadowPhase.run` awaits once per gate-suppressed region. The box
/// (rather than a bare closure field) keeps the `RegionShadowPhase.Input`
/// declaration readable and mirrors how the shadow path injects behavior.
///
/// `RegionShadowPhase` stays a pure static enum: the async FM work lives
/// entirely inside this closure / its backing dispatcher, so no actor is
/// introduced into the region pipeline.
struct FMRegionRecoveryClassifier: Sendable {
    let classify: @Sendable (_ region: ProposedRegion, _ atoms: [TranscriptAtom]) async -> FMRegionVerdict

    init(
        _ classify: @escaping @Sendable (_ region: ProposedRegion, _ atoms: [TranscriptAtom]) async -> FMRegionVerdict
    ) {
        self.classify = classify
    }
}

// MARK: - FMRegionRecoveryDispatcher

/// Protocol for the injected dispatcher that owns the actual FM call.
/// `AdDetectionService` holds an optional one (nil in tests / preview /
/// FM-unavailable) and, when present, adapts it into the `@Sendable`
/// recovery closure at the `RegionShadowPhase.Input` site. Tests substitute
/// a stub conformer; production wires `LiveFMRegionRecoveryDispatcher`.
protocol FMRegionRecoveryDispatcher: Sendable {
    func classify(region: ProposedRegion, atoms: [TranscriptAtom]) async -> FMRegionVerdict
}

// MARK: - WindowSweep

/// Configures how many coarse FM windows the live dispatcher queries per
/// gate-suppressed region, and how far apart their onset starts are stepped.
///
/// `windowCount == 1` (`.single`, the DEFAULT) is playhead-r2vz's Option A:
/// exactly one window at the region's music→speech edge — byte-identical to
/// the shipped single-window dispatcher.
///
/// `windowCount > 1` (playhead-vlo1, Option B) is the sliding-window sweep:
/// `K` overlapping coarse windows, each ~one-ad-read wide (the same
/// `onsetWindowCharacterCap` per window), whose onset starts are stepped
/// forward from the edge by `strideSeconds` (window `i` starts at
/// `region.endTime - padLeadSeconds + i · strideSeconds`). The dispatcher
/// admits on the FIRST window that returns `.containsAd` and short-circuits
/// the rest. Rationale: a single fixed window sits on the FM capability floor;
/// offset windows catch a delayed / differently-framed ad-read (the
/// "sliding-window recovers host-reads 5/6" evidence). Opt-in, set only for
/// measurement — production keeps `.single`.
///
/// `strideSeconds` and `windowCount` are the ONLY new tuning knobs; window
/// WIDTH still reuses the gate's `onsetWindowCharacterCap` (no invented magic).
struct WindowSweep: Sendable, Equatable {
    /// Number of overlapping coarse windows queried per region (clamped to ≥ 1).
    let windowCount: Int
    /// Forward step (seconds) between consecutive window onset starts, measured
    /// from the music→speech edge. Ignored when `windowCount == 1`.
    let strideSeconds: Double

    init(windowCount: Int = 1, strideSeconds: Double = 15) {
        self.windowCount = max(1, windowCount)
        // A negative stride would step BACKWARD and collapse the sweep onto the
        // edge window; clamp to forward-only. `windowCount > 1` only does useful
        // work with `strideSeconds > 0`.
        self.strideSeconds = max(0, strideSeconds)
    }

    /// Single-window Option A — the DEFAULT. One window at the music→speech
    /// edge; byte-identical to playhead-r2vz.
    static let single = WindowSweep(windowCount: 1, strideSeconds: 0)

    /// A sensible ~3-window sliding sweep (Option B). Overlapping half-strides
    /// stepped forward from the edge. Opt-in for measurement only.
    static let sweep = WindowSweep(windowCount: 3, strideSeconds: 15)
}

// MARK: - LiveFMRegionRecoveryDispatcher

/// Live recovery dispatcher backed by a fresh `FoundationModelClassifier.Runtime`
/// session per region. Serializes through actor isolation (concurrency = 1),
/// exactly like `LiveShadowFMDispatcher`.
actor LiveFMRegionRecoveryDispatcher: FMRegionRecoveryDispatcher {

    private let runtime: FoundationModelClassifier.Runtime
    /// Redactor applied to per-segment text before it lands in the coarse
    /// prompt. Defaults to `.noop` — matching the production classifier's
    /// default when `PLAYHEAD_FM_REDACT` is unset — so the recovery prompt is
    /// the plain champion `.classification` coarse prompt.
    private let redactor: PromptRedactor
    /// Prompt prefix used to prewarm the session before the coarse submit.
    /// Matches `FoundationModelClassifier`'s coarse `promptPrefix` so the
    /// runtime's prewarm-cache semantics align with the production coarse path.
    private let prewarmPrefix: String
    /// Seconds of lead BEFORE the music→speech edge (`region.endTime`) at which
    /// the post-edge onset window starts. Mirrors the gate's
    /// `onsetWindowLeadSeconds`, so the FM reads the SAME window the lexical
    /// gate inspected — a beat before the ad-read begins onward (see
    /// `regionSegments`).
    private let padLeadSeconds: Double
    /// How many overlapping coarse windows to sweep per region, and their
    /// forward stride. Defaults to `.single` — one window at the edge,
    /// byte-identical to playhead-r2vz (Option A). `.sweep` (or any
    /// `windowCount > 1`) enables the playhead-vlo1 sliding-window sweep.
    private let sweep: WindowSweep
    private let logger: Logger

    init(
        runtime: FoundationModelClassifier.Runtime,
        redactor: PromptRedactor = .noop,
        prewarmPrefix: String = "Classify ad content.",
        padLeadSeconds: Double = MusicOffsetLexicalGate.onsetWindowLeadSeconds,
        sweep: WindowSweep = .single,
        logger: Logger = Logger(
            subsystem: "com.playhead",
            category: "FMRegionRecoveryDispatcher"
        )
    ) {
        self.runtime = runtime
        self.redactor = redactor
        self.prewarmPrefix = prewarmPrefix
        self.padLeadSeconds = padLeadSeconds
        self.sweep = sweep
        self.logger = logger
    }

    // MARK: - FMRegionRecoveryDispatcher

    func classify(region: ProposedRegion, atoms: [TranscriptAtom]) async -> FMRegionVerdict {
        // Sweep K overlapping coarse windows stepped forward from the region's
        // music→speech edge and ADMIT ON ANY `.containsAd`. `windowCount == 1`
        // (`.single`, the default) degenerates to exactly the playhead-r2vz
        // single-window path: one window at offset 0, one `respondCoarse` call,
        // the same verdict. `windowCount > 1` (playhead-vlo1) adds forward-
        // stepped windows to clear FM's single-shot capability floor.
        //
        // Aggregation over the sweep:
        //   - the FIRST window that returns `.containsAd` → `.ad` (short-circuit:
        //     later windows are NOT queried, saving FM calls);
        //   - otherwise, if ANY window returned a real no-ad read
        //     (`.noAds`/`.uncertain` → `.content`) → `.content` (stay suppressed);
        //   - if EVERY window degraded (`.abstain`/throw/empty → `.unavailable`)
        //     → `.unavailable` (graceful degrade, region stays suppressed).
        var sawContent = false
        var queriedSignatures = Set<[Int]>()
        for index in 0..<max(1, sweep.windowCount) {
            let forwardOffsetSeconds = Double(index) * sweep.strideSeconds
            let segments = Self.regionSegments(
                region: region,
                atoms: atoms,
                padLeadSeconds: padLeadSeconds,
                forwardOffsetSeconds: forwardOffsetSeconds
            )
            // No transcript text in this window → nothing to screen; skip it
            // WITHOUT a model call. When every window is empty (region carries
            // no atoms at all) the loop falls through to `.unavailable` with
            // zero `respondCoarse` calls — unchanged from the single-window path.
            guard !segments.isEmpty else { continue }

            // Overlapping windows can select the SAME atom set when post-edge
            // atoms are sparse (or the sweep steps past the last atom). Query
            // each DISTINCT window at most once: a repeat window carries
            // identical content, so re-querying it cannot change the verdict —
            // it would only burn an FM call (and, under any residual FM non-
            // determinism, add a spurious re-roll to the admit-on-any race).
            // Recall is unchanged; only genuinely different windows — the whole
            // point of the sweep — reach the model. On real dense transcripts
            // the K offset windows are distinct, so this trims only degenerate
            // repeats; at `windowCount == 1` the single window always inserts,
            // preserving the byte-identical one-call default.
            let signature = segments
                .flatMap { $0.atoms }
                .map { $0.atomKey.atomOrdinal }
                .sorted()
            guard queriedSignatures.insert(signature).inserted else { continue }

            // Reuse the champion `.classification` coarse prompt verbatim — do
            // NOT hand-roll a new prompt (DECISION: PR2 charter).
            let prompt = FoundationModelClassifier.buildPrompt(for: segments, redactor: redactor)

            // Fresh session per window, prewarmed — mirrors the coarse
            // production path and `LiveShadowFMDispatcher`. Sessions are
            // serialized through this actor (concurrency = 1). Discarded when
            // the call returns.
            let session = await runtime.makeSession()
            await session.prewarm(prewarmPrefix)

            do {
                let response = try await session.respondCoarse(prompt)
                switch Self.verdict(for: response.disposition) {
                case .ad:
                    // Admit-on-any + short-circuit: one window says ad → done.
                    return .ad
                case .content:
                    sawContent = true
                case .unavailable:
                    // This window degraded (abstain); keep sweeping the rest.
                    break
                }
            } catch {
                // Refusal / decoding failure / throttle / runtime-unavailable —
                // graceful degrade for THIS window; keep sweeping the rest.
                logger.warning(
                    """
                    FM region recovery failed: \
                    region=\(region.firstAtomOrdinal, privacy: .public)..\(region.lastAtomOrdinal, privacy: .public) \
                    window=\(index, privacy: .public) \
                    error=\(String(describing: error), privacy: .public)
                    """
                )
            }
        }
        // No window admitted. If at least one returned a real no-ad read stay
        // suppressed as `.content`; if every window degraded, `.unavailable`.
        return sawContent ? .content : .unavailable
    }

    // MARK: - Mapping (pure, unit-tested directly)

    /// Map the coarse screening disposition to a recovery verdict.
    ///
    ///   - `.containsAd` → `.ad`          (re-admit).
    ///   - `.noAds`      → `.content`     (stay suppressed).
    ///   - `.uncertain`  → `.content`     (DECISION #3: conservative — an
    ///                       uncertain FM read does NOT resurrect a banner the
    ///                       lexical gate already flagged as cue-less).
    ///   - `.abstain`    → `.unavailable` (graceful degrade, stay suppressed).
    static func verdict(for disposition: CoarseDisposition) -> FMRegionVerdict {
        switch disposition {
        case .containsAd:
            return .ad
        case .noAds:
            return .content
        case .uncertain:
            return .content
        case .abstain:
            return .unavailable
        }
    }

    // MARK: - Region → segments

    /// Build the `[AdTranscriptSegment]` the FM sees for `region`.
    ///
    /// CRITICAL windowing note: a t1py `.sustainedMusic` region is the music
    /// PLAY-OUT span `[runStart, trailingEdge)` — its `endTime` IS the
    /// music→speech edge, and the predicate only fires on instrumental music
    /// (speech-over-music breaks it; see `SustainedMusicOffsetProposer`). So the
    /// discriminating ad-read is the speech AFTER `region.endTime` — exactly the
    /// window the lexical gate inspected and found cue-less
    /// (`MusicOffsetLexicalGate.onsetWindowText(trailingEdge: region.endTime)`).
    /// Segmenting only `[firstAtomOrdinal, lastAtomOrdinal]` would hand the FM
    /// the music play-out, NOT the ad — defeating the recovery. We therefore
    /// build the window as:
    ///   1. the region's own atoms (the music tail — boundary context), plus
    ///   2. the POST-EDGE onset window: atoms after the region whose read starts
    ///      at/after `region.endTime - padLeadSeconds`, capped at the gate's
    ///      `onsetWindowCharacterCap` so the single coarse call stays bounded.
    /// This reuses the gate's own constants (no new tuning), so the FM reads the
    /// same span the gate scored — only with FM instead of lexical patterns,
    /// which is the whole point (lexical = attention, FM disposes). Segmented
    /// via `TranscriptSegmenter.segment` (the production coarse segmenter).
    /// `atoms` is the full episode atom stream; this filters it.
    ///
    /// The restored region itself is UNMODIFIED — this window only feeds the
    /// ad/no-ad DECISION; the banner keeps the region's original music-span
    /// width (markOnly-by-omission).
    ///
    /// `forwardOffsetSeconds` steps the POST-EDGE onset window forward for the
    /// sliding-window sweep (playhead-vlo1): window `i` uses
    /// `forwardOffsetSeconds = i · strideSeconds`, so its onset start is
    /// `region.endTime - padLeadSeconds + forwardOffsetSeconds`. `0` (the
    /// default) is Option A's single window at the edge — byte-identical to
    /// playhead-r2vz. The music-tail region atoms are included in EVERY window
    /// (shared boundary context); only the post-edge onset slice slides.
    static func regionSegments(
        region: ProposedRegion,
        atoms: [TranscriptAtom],
        padLeadSeconds: Double,
        forwardOffsetSeconds: Double = 0
    ) -> [AdTranscriptSegment] {
        let sorted = atoms.sorted { $0.atomKey.atomOrdinal < $1.atomKey.atomOrdinal }
        let onsetStart = region.endTime - max(0, padLeadSeconds) + forwardOffsetSeconds
        let charCap = MusicOffsetLexicalGate.onsetWindowCharacterCap
        var selected: [TranscriptAtom] = []
        var onsetChars = 0
        for atom in sorted {
            let ordinal = atom.atomKey.atomOrdinal
            // 1) The region's own atoms — always included (music-tail context).
            if ordinal >= region.firstAtomOrdinal && ordinal <= region.lastAtomOrdinal {
                selected.append(atom)
                continue
            }
            // 2) The post-edge onset window — the ad-read — char-capped so the
            //    forward extent stays within the coarse token budget.
            if ordinal > region.lastAtomOrdinal,
               atom.startTime >= onsetStart,
               onsetChars < charCap {
                selected.append(atom)
                onsetChars += atom.text.count
            }
        }
        return TranscriptSegmenter.segment(atoms: selected)
    }
}
