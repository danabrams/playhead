// PostRollEndClamp.swift
// playhead-aqo9: the post-roll end-at-EOF clamp — the mirror image of
// `PreRollStartClamp`, and the larger half of the free-edge width win.
//
// WHY
// ---
// A post-roll ad runs to the end of the file. The detector routinely stops
// short of it: the transcript thins out over the outro, the presence core
// decays, and the marked window ends tens of seconds before EOF. A post-roll's
// END edge is "free" at the episode end — there is no editorial content after
// it to clip — so when the episode's LAST ad slot already ends NEAR the end of
// the file, its end edge is extended to exactly `episodeDuration`.
//
// MEASURED (playhead-aqo9, against Dan's own corrections on the device):
// post-roll truth ends within 2.23 s of EOF in 6/6 pods, all six from
// chunk-exact user marks — the strongest single result in that measurement.
// The detector left 233.4 s of that audible across 8 episodes.
//
// THE PROXIMITY GUARD — why this clamp needs one and the pre-roll clamp does not
// -----------------------------------------------------------------------------
// `PreRollStartClamp` can lean on t=0: an episode's first ad slot either IS the
// pre-roll or starts minutes in, and the ceiling separates those cleanly. There
// is no such luck at the tail. "The last visible window" is the post-roll only
// when the detector actually found the post-roll; when it did not, the last
// window is some unrelated mid-roll and snapping ITS end to EOF claims
// everything in between as an ad.
//
// That is not hypothetical. On the measured corpus the unguarded form recovered
// 251 s of ad but claimed 3,150 s of show — one episode alone contributed 2,813 s
// because its last window sat at 1:59 in a 48-minute file. The guard is what
// makes the snap safe: the detected end must ALREADY be within
// `maxEndDistanceSeconds` of EOF before it is extended there.
//
// The threshold is not knife-edge. On Dan's device the last-visible-window
// distance-to-EOF splits into two clean groups with nothing between them:
// genuine post-rolls at 0.0/0.5/1.0/1.6/31.7/38.1/40.2 s, and non-post-rolls at
// 159.4 s and beyond. Every value from 45 s to 120 s produces byte-identical
// recovery (76.1 s of ad, 3.8 s of non-ad, 20:1); 180 s falls off the cliff
// (206.9 s ad against 195.4 s of show, 1:1). 60 s sits inside that plateau with
// 1.5x margin over the widest true gap and 2.6x headroom below the narrowest
// false one.
//
// SCOPE (pinned — identical to the pre-roll clamp)
// -----------------------------------------------
// A WIDTH / MARK improvement ONLY, applied AFTER the per-span decision loop.
// The widened suffix was never classified, so every changed window is capped to
// mark-only (or keeps a stricter existing gate); confidence and lifecycle
// remain diagnostic, never automatic authority.
//
// TRUSTWORTHY EDGES ARE EXEMPT: only an `.unanchored` END edge is clamped. A
// byte-exact rediff edge or a stinger-snapped edge already located the boundary
// PRECISELY (and a rediff edge IS the deterministic auto-skip range), so the
// clamp must not override it.
//
// It touches ONLY the episode's last ad slot. Earlier slots are never clamped,
// and a last slot whose end is further than `maxEndDistanceSeconds` from EOF is
// not the post-roll — there is nothing free to extend — so it is left alone.
//
// INNER EDGES ARE NOT TOUCHED. The same measurement found the post-roll START
// over-reaching a median 97.9 s back into the show, corroborated by Dan's own
// vetoes — but at four slots that is too thin to move an edge that abuts
// content. The start edge is deliberately left exactly where the detector put
// it (playhead-aqo9 scope note 3).
//
// INVARIANTS
// ----------
//   • Idempotent: an end already at (or past) `episodeDuration` is a no-op, so
//     `clamp(clamp(x)) == clamp(x)`.
//   • Monotonic: the end only ever moves RIGHTWARD → coverage never shrinks and
//     the end can never precede the start.
//   • Order-preserving: the clamped window keeps its array position and its
//     ordinal-addressed id, so a downstream content-addressed reconcile stays in
//     place and no slot is reordered.
//   • Never whole-episode: a window that already reaches 0.0 is refused, so no
//     single mark can be widened to cover the entire file.
//
// PURITY: pure functions over value types, `Foundation` only, deterministic, no
// I/O and no actor hops.

import Foundation

enum PostRollEndClamp {

    // MARK: - Configuration

    struct Configuration: Sendable, Equatable {
        /// How close (seconds) the episode's LAST ad slot must ALREADY end to
        /// the end of the file before its end edge is extended to
        /// `episodeDuration`.
        ///
        /// A last slot ending within `maxEndDistanceSeconds` of EOF is the
        /// post-roll the outro decay cut short → clamp it. A last slot ending
        /// further out is some earlier break the detector happened to find last
        /// → leave it untouched, because everything between it and EOF is show.
        /// `<= 0` disables the clamp entirely.
        ///
        /// Default `60.0`, measured — see the file header for the sweep.
        var maxEndDistanceSeconds: Double

        static let `default` = Configuration(maxEndDistanceSeconds: 60.0)

        init(maxEndDistanceSeconds: Double = 60.0) {
            self.maxEndDistanceSeconds = maxEndDistanceSeconds
        }
    }

    // MARK: - Public API

    /// Return `windows` with the episode's last ad slot's end edge extended to
    /// exactly `episodeDuration` when that slot already ends within
    /// `maxEndDistanceSeconds` of it.
    ///
    /// The "last ad slot" is the latest-ending VISIBLE (non-suppressed) window —
    /// a suppressed window is never shown, so it is not the post-roll mark; ties
    /// break by latest start, then id, for determinism. The last slot is clamped
    /// ONLY when its end edge is `.unanchored`; a trustworthy
    /// `.rediffByteExact` / `.stingerSnapped` end is left untouched. Everything
    /// else is unchanged: the START edge, every other window, the array
    /// ordering, and every non-authority field of the clamped window (`id`,
    /// `decisionState`, `confidence`, `evidenceStartTime`, …). An eligible or
    /// missing gate is demoted to mark-only; a stricter gate is preserved.
    ///
    /// - Parameters:
    ///   - windows: the episode's finalized ad windows (any order).
    ///   - episodeDuration: the episode's full length in seconds. A
    ///     non-positive or non-finite duration disables the clamp — without a
    ///     trustworthy EOF there is no edge to snap to.
    ///   - config: the proximity guard. `maxEndDistanceSeconds <= 0` disables.
    /// - Returns: `windows` with at most the last slot's end clamped to
    ///   `episodeDuration`.
    static func clamp(
        windows: [AdWindow],
        episodeDuration: Double,
        config: Configuration = .default
    ) -> [AdWindow] {
        // Disabled: a non-positive threshold means "never clamp".
        guard config.maxEndDistanceSeconds.isFinite,
              config.maxEndDistanceSeconds > 0 else {
            return windows
        }

        // No trustworthy EOF → nothing to snap to. `runBackfill` is called with
        // `episodeDuration: 0` in a few degenerate paths; fail closed there
        // rather than clamping every window's end to 0.
        //
        // DEFENCE IN DEPTH, and honestly labelled as such: this guard is
        // UNREACHABLE given the three below it, so deleting it does not fail any
        // test (a mutation confirmed that). The proof: clamping requires
        // `startTime > 0`, `endTime > startTime` and `endTime < episodeDuration`,
        // which together imply `episodeDuration > 0`; a NaN duration loses
        // `endTime < episodeDuration`; and an infinite duration loses the
        // proximity comparison. It is kept because that argument runs through
        // THREE separate guards and IEEE NaN semantics — relying on it
        // implicitly is how a later reorder silently starts clamping every
        // window's end to zero. Do not "clean this up" by deleting it; if the
        // redundancy ever bothers you, delete it together with a test proving
        // the composite still fails closed.
        guard episodeDuration.isFinite, episodeDuration > 0 else {
            return windows
        }

        let suppressedState = AdDecisionState.suppressed.rawValue

        // Last ad slot = the latest-ENDING VISIBLE window. Computed over indices
        // so array order is irrelevant. Ties: latest start, then id. This is the
        // exact time-reversed mirror of `PreRollStartClamp`'s first-slot pick.
        let lastIndex = windows.indices
            .filter { windows[$0].decisionState != suppressedState }
            .max { lhs, rhs in
                let a = windows[lhs]
                let b = windows[rhs]
                if a.endTime != b.endTime { return a.endTime < b.endTime }
                if a.startTime != b.startTime { return a.startTime < b.startTime }
                return a.id < b.id
            }
        guard let index = lastIndex else { return windows }

        let last = windows[index]

        // Trustworthy-edge exemption: only an UNANCHORED end is clamped. A
        // byte-exact rediff edge or a stinger-snapped edge located the boundary
        // PRECISELY — and, for rediff, that edge IS the deterministic auto-skip
        // range — so overriding it could claim post-ad content (a sign-off, a
        // trailer for the next episode) as ad and widen a cross-session skip
        // over real material. The outro decay this clamp repairs only ever
        // afflicts the FM / lexical / presence-core guesses that carry the
        // `.unanchored` tag.
        guard last.endEdgeAnchor == AutoSkipEdgeAnchor.unanchored.rawValue else {
            return windows
        }

        // USER-SET BOUNDARIES ARE NOT OURS TO MOVE. `.unanchored` above means
        // "no DETECTOR anchored this edge" — it does NOT mean "no human chose
        // it". A window the listener added carries no edge anchor, so it reaches
        // this point looking exactly like an FM guess, and without this guard the
        // clamp extends the end the listener selected by hand.
        //
        // That is forbidden by the fidelity rule: a manual mark outranks
        // anything else, and a transcript span marking is the HIGHEST-fidelity
        // correction precisely because the time bounds are the listener's rather
        // than the detector's. This clamp is a derived positional heuristic and
        // sits below every correction source on that ladder, so it must defer.
        //
        // Found by `UserAddedMarkSurvivesBackfillTests` failing: a hand-marked
        // 20–30 s window in a 90 s episode is exactly 60 s from EOF, so the
        // proximity guard admitted it and the clamp moved the user's end to 90.
        //
        // The literal rather than `AdBoundaryState.userMarked` is deliberate:
        // that enum has no such case, and the value is written and read as a
        // raw string throughout (`AdDetectionService.swift:2939` writes it,
        // `TranscriptPeekViewModel.swift:441` reads it). Matching the existing
        // convention here rather than adding a case, because adding one touches
        // every exhaustive switch over that enum and is not this bead's work.
        guard last.boundaryState != "userMarked" else {
            return windows
        }

        // Geometry gate.
        //   • `endTime < episodeDuration` — an end already at/past EOF has
        //     nothing to do (idempotent no-op).
        //   • `episodeDuration - endTime <= maxEndDistanceSeconds` — THE
        //     PROXIMITY GUARD. A further-out end is not the post-roll, so
        //     everything between it and EOF is show, not ad.
        guard last.startTime.isFinite,
              last.endTime.isFinite,
              last.endTime > last.startTime,
              last.endTime < episodeDuration,
              episodeDuration - last.endTime <= config.maxEndDistanceSeconds else {
            return windows
        }

        // Whole-episode refusal. If this slot already reaches 0.0 — because the
        // episode has a single ad window, or because `PreRollStartClamp` ran
        // first and widened it — then extending its end to EOF would mark the
        // ENTIRE FILE as an ad. No evidence supports that mark and the cost of
        // being wrong is the whole episode, so refuse. This is the one guard
        // with no counterpart in the pre-roll clamp, which runs first and
        // therefore cannot hit the composed case.
        guard last.startTime > 0 else { return windows }

        var result = windows
        result[index] = last.withEndTimeClamped(to: episodeDuration)
        return result
    }
}

// MARK: - AdWindow copy helper

private extension AdWindow {
    /// A copy of this window with `endTime` moved to `duration`.
    ///
    /// The ordinal-addressed `id`, `decisionState`, `confidence`, `startTime`
    /// and `evidenceStartTime` are copied unchanged — the clamp widens the MARK,
    /// not the evidence, and never the inner edge. The gate is capped to
    /// mark-only because the newly added suffix has no classifier authority.
    /// Catalog-match provenance is also cleared: it describes the old
    /// `[startTime, endTime)` fingerprint, and retaining its exact row identity
    /// after widening would let a later correction revoke unrelated source
    /// material.
    func withEndTimeClamped(to duration: Double) -> AdWindow {
        let safeEligibilityGate: String
        if let existing = eligibilityGate.flatMap(
            SkipEligibilityGate.init(rawValue:)
        ),
           existing.severity >= SkipEligibilityGate.markOnly.severity {
            safeEligibilityGate = existing.rawValue
        } else {
            safeEligibilityGate = SkipEligibilityGate.markOnly.rawValue
        }
        return AdWindow(
            id: id,
            analysisAssetId: analysisAssetId,
            startTime: startTime,
            endTime: duration,
            confidence: confidence,
            boundaryState: boundaryState,
            decisionState: decisionState,
            detectorVersion: detectorVersion,
            advertiser: advertiser,
            product: product,
            adDescription: adDescription,
            evidenceText: evidenceText,
            evidenceStartTime: evidenceStartTime,
            metadataSource: metadataSource,
            metadataConfidence: metadataConfidence,
            metadataPromptVersion: metadataPromptVersion,
            wasSkipped: wasSkipped,
            userDismissedBanner: userDismissedBanner,
            evidenceSources: evidenceSources,
            eligibilityGate: safeEligibilityGate,
            catalogStoreMatchSimilarity: nil,
            catalogFingerprintVersion: nil,
            catalogMatchedEntryId: nil,
            catalogMatchedShowId: nil,
            catalogMatchedLearningSource: nil,
            catalogMatchedLearningLifecycle: nil,
            startEdgeAnchor: startEdgeAnchor,
            endEdgeAnchor: endEdgeAnchor
        )
    }
}
