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
    private let logger: Logger

    init(
        runtime: FoundationModelClassifier.Runtime,
        redactor: PromptRedactor = .noop,
        prewarmPrefix: String = "Classify ad content.",
        padLeadSeconds: Double = MusicOffsetLexicalGate.onsetWindowLeadSeconds,
        logger: Logger = Logger(
            subsystem: "com.playhead",
            category: "FMRegionRecoveryDispatcher"
        )
    ) {
        self.runtime = runtime
        self.redactor = redactor
        self.prewarmPrefix = prewarmPrefix
        self.padLeadSeconds = padLeadSeconds
        self.logger = logger
    }

    // MARK: - FMRegionRecoveryDispatcher

    func classify(region: ProposedRegion, atoms: [TranscriptAtom]) async -> FMRegionVerdict {
        let segments = Self.regionSegments(
            region: region,
            atoms: atoms,
            padLeadSeconds: padLeadSeconds
        )
        // No transcript text in the region window → nothing to screen. Treat
        // as unavailable so the region stays suppressed (PR1 behavior).
        guard !segments.isEmpty else { return .unavailable }

        // Reuse the champion `.classification` coarse prompt verbatim — do NOT
        // hand-roll a new prompt (DECISION: PR2 charter).
        let prompt = FoundationModelClassifier.buildPrompt(for: segments, redactor: redactor)

        // Fresh session per region, prewarmed — mirrors the coarse production
        // path and `LiveShadowFMDispatcher`. Discarded when the call returns.
        let session = await runtime.makeSession()
        await session.prewarm(prewarmPrefix)

        do {
            let response = try await session.respondCoarse(prompt)
            return Self.verdict(for: response.disposition)
        } catch {
            // Refusal / decoding failure / throttle / runtime-unavailable —
            // graceful degrade to `.unavailable` (region stays suppressed).
            logger.warning(
                "FM region recovery failed: region=\(region.firstAtomOrdinal, privacy: .public)..\(region.lastAtomOrdinal, privacy: .public) error=\(String(describing: error), privacy: .public)"
            )
            return .unavailable
        }
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
    static func regionSegments(
        region: ProposedRegion,
        atoms: [TranscriptAtom],
        padLeadSeconds: Double
    ) -> [AdTranscriptSegment] {
        let sorted = atoms.sorted { $0.atomKey.atomOrdinal < $1.atomKey.atomOrdinal }
        let onsetStart = region.endTime - max(0, padLeadSeconds)
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
