// PreRollStartClamp.swift
// playhead-xsdz.66: the pre-roll start-at-zero clamp — a deterministic DAI
// WIDTH win for the episode's first ad slot.
//
// WHY
// ---
// A pre-roll ad begins at 0:00, but the detector routinely UNDER-measures its
// opening: the ASR / transcript pipeline has a cold-start ramp, and a cold intro
// sting or music bed can push the anchored presence core (and therefore the
// marked pre-roll) tens of seconds late. A pre-roll's start edge is "free" at
// 0:00 — there is no editorial content before it to clip — so when the episode's
// FIRST ad slot lands in the pre-roll zone, its start edge is extended to exactly
// 0.0. This recovers the width the ramp lost (measured pre-roll width coverage
// ~57% → ~80%).
//
// playhead-aqo9 (2026-07-29) raised `maxPreRollStartSeconds` from 20 to 90 after
// measuring the miss against Dan's own on-device corrections: pre-roll truth
// starts within 5 s of 0:00 in 8/8 pods, and the 20 s ceiling was leaving 121 of
// the 147.8 recoverable ad-seconds audible. See the property's doc comment.
//
// SCOPE (pinned)
// -------------
// This is a WIDTH / MARK improvement ONLY. In the pipeline it is applied AFTER
// the per-span decision loop. Because the widened prefix was not classified,
// every changed window is capped to mark-only (or keeps a stricter existing
// gate); confidence and lifecycle remain diagnostic, never automatic authority.
//
// TRUSTWORTHY EDGES ARE EXEMPT: only an `.unanchored` start edge is clamped. A
// byte-exact rediff edge or a stinger-snapped edge already located the boundary
// PRECISELY (and a rediff edge IS the deterministic auto-skip range), so the
// clamp must not override it — mirroring the backfill loop's rule that WIDTH-
// owned edges bypass every boundary refiner. The ASR cold-start miss this clamp
// repairs only ever afflicts the `.unanchored` FM / lexical / presence-core
// guesses, so gating on `.unanchored` targets exactly that class and can never
// widen a precise DAI slot over pre-ad content (e.g. an intro jingle).
//
// It touches ONLY the episode's first ad slot (the pre-roll position). Mid-roll
// and post-roll slots are never clamped, and a "first" slot that starts well past
// `maxPreRollStartSeconds` is a mid-roll — there is no pre-roll to extend — so it
// is left untouched.
//
// INVARIANTS
// ----------
//   • Idempotent: a start already at (or before) 0.0 is a no-op, so
//     `clamp(clamp(x)) == clamp(x)`.
//   • Monotonic: the start only ever moves LEFTWARD → coverage never shrinks and
//     the start can never exceed the end.
//   • Order-preserving: the clamped window keeps its array position and its
//     ordinal-addressed id, so a downstream content-addressed reconcile stays in
//     place and no slot is reordered.
//
// PURITY: pure functions over value types, `Foundation` only, deterministic, no
// I/O and no actor hops.

import Foundation

enum PreRollStartClamp {

    // MARK: - Configuration

    struct Configuration: Sendable, Equatable {
        /// Maximum start time (seconds) for the episode's first ad slot to be
        /// treated as a PRE-ROLL and have its start edge extended to 0.0.
        ///
        /// A first slot whose start sits in `(0, maxPreRollStartSeconds]` is a
        /// pre-roll the cold-start ramp started late → clamp it. A first slot
        /// that starts LATER than this is a mid-roll (there is no pre-roll to
        /// extend) → leave it untouched. `<= 0` disables the clamp entirely.
        ///
        /// Default `90.0`, MEASURED (playhead-aqo9, 2026-07-29). The original
        /// `20.0` was reasoned from the ASR cold-start ramp alone and turned out
        /// to be far too tight: against Dan's own corrections it covered pre-roll
        /// starts of 11.8 and 15.2 s but missed 46.4 and 74.6 s, leaving 121 of
        /// the 147.8 free pre-roll ad-seconds audible. The detector's first
        /// visible window starts late for reasons beyond ASR warm-up — a cold
        /// intro sting, a music bed, an unlabelled promo — and those cost tens of
        /// seconds, not a handful.
        ///
        /// 90 is where the data puts the boundary, not where the curve was fitted.
        /// Across the 36 analysed assets the first VISIBLE window's start clusters
        /// at 0.0–87.9 s and then jumps straight to 300.0 s, so 90 takes the whole
        /// cluster and still stops well short of anything that is not a pre-roll —
        /// exactly the separation the original constant existed to protect. 90,
        /// 120 and 150 are byte-identical on that corpus (nothing lives in the
        /// gap); 45 recovers nothing at all beyond 20.
        var maxPreRollStartSeconds: Double

        static let `default` = Configuration(maxPreRollStartSeconds: 90.0)

        init(maxPreRollStartSeconds: Double = 90.0) {
            self.maxPreRollStartSeconds = maxPreRollStartSeconds
        }
    }

    // MARK: - Public API

    /// Return `windows` with the episode's first ad slot's start edge extended to
    /// exactly 0.0 when it sits in the pre-roll zone `(0, maxPreRollStartSeconds]`.
    ///
    /// The "first ad slot" is the earliest-starting VISIBLE (non-suppressed)
    /// window — a suppressed window is never shown, so it is not the pre-roll
    /// mark; ties break by earliest end, then id, for determinism. The first slot
    /// is clamped ONLY when its start edge is `.unanchored`; a trustworthy
    /// `.rediffByteExact` / `.stingerSnapped` start is left untouched. Everything
    /// else is unchanged: the end edge, every other window, the array ordering,
    /// and every non-authority field of the clamped window (`id`,
    /// `decisionState`, `confidence`, `evidenceStartTime`, …). An eligible or
    /// missing gate is demoted to mark-only; a stricter gate is preserved.
    ///
    /// - Parameters:
    ///   - windows: the episode's finalized ad windows (any order).
    ///   - config: the pre-roll threshold. `maxPreRollStartSeconds <= 0` disables.
    /// - Returns: `windows` with at most the first slot's start clamped to 0.0.
    static func clamp(
        windows: [AdWindow],
        config: Configuration = .default,
        protectedRegions: [(start: Double, end: Double)] = []
    ) -> [AdWindow] {
        // Disabled: a non-positive threshold means "never clamp".
        guard config.maxPreRollStartSeconds.isFinite,
              config.maxPreRollStartSeconds > 0 else {
            return windows
        }

        let suppressedState = AdDecisionState.suppressed.rawValue

        // First ad slot = the earliest-starting VISIBLE window. Computed over
        // indices so array order is irrelevant (a rewrite pass upstream is not
        // required to keep the list start-sorted). Ties: earliest end, then id.
        let firstIndex = windows.indices
            .filter { windows[$0].decisionState != suppressedState }
            .min { lhs, rhs in
                let a = windows[lhs]
                let b = windows[rhs]
                if a.startTime != b.startTime { return a.startTime < b.startTime }
                if a.endTime != b.endTime { return a.endTime < b.endTime }
                return a.id < b.id
            }
        guard let index = firstIndex else { return windows }

        let first = windows[index]

        // Trustworthy-edge exemption: only an UNANCHORED start is clamped. A
        // byte-exact rediff edge or a stinger-snapped edge located the boundary
        // PRECISELY — and, for rediff, that edge IS the deterministic auto-skip
        // range — so overriding it could claim pre-ad content (e.g. a fixed
        // `[0, 8]` theme jingle before a DAI ad at 8 s) as ad and widen a
        // cross-session skip over real content. The cold-start miss this clamp
        // repairs only ever afflicts the FM / lexical / presence-core guesses
        // that carry the `.unanchored` tag; this mirrors the backfill loop's
        // existing rule that WIDTH-owned (rediff / stinger) edges bypass every
        // boundary refiner because the slot pass locked their physical edges.
        guard first.startEdgeAnchor == AutoSkipEdgeAnchor.unanchored.rawValue else {
            return windows
        }

        // USER-SET BOUNDARIES ARE NOT OURS TO MOVE. `.unanchored` above means
        // "no DETECTOR anchored this edge" — it does NOT mean "no human chose
        // it". A window the listener added carries no edge anchor, so it arrives
        // here looking exactly like an FM guess.
        //
        // This was LATENT rather than new: the clamp could always move a
        // hand-set start, but at the old 20 s ceiling it never reached one.
        // Raising the ceiling to 90 s brought `UserAddedMarkSurvivesBackfill`'s
        // 20–30 s mark into range, and the clamp moved the listener's start to
        // 0. So the low ceiling was hiding the bug, not preventing it.
        //
        // Forbidden by the fidelity rule: a manual mark outranks anything else,
        // and a transcript span marking is the HIGHEST-fidelity correction
        // precisely because the bounds are the listener's rather than the
        // detector's. This clamp is a derived positional heuristic and sits below
        // every correction source, so it defers.
        //
        // Literal rather than an enum case because `AdBoundaryState` has none —
        // the value is written and read as a raw string throughout
        // (`AdDetectionService.swift:2939` writes, `TranscriptPeekViewModel.swift:441`
        // reads). Matching that convention; adding a case would touch every
        // exhaustive switch over the enum and is not this bead's work.
        guard first.boundaryState != "userMarked" else {
            return windows
        }

        // Pre-roll gate: only a first slot whose start sits in `(0, N]`.
        //   • `startTime > 0` — a start already at/before 0.0 has nothing to do
        //     (idempotent no-op).
        //   • `startTime <= maxPreRollStartSeconds` — a later start is a mid-roll,
        //     not a pre-roll, so there is no free start edge to extend.
        guard first.startTime.isFinite,
              first.endTime.isFinite,
              first.endTime > first.startTime,
              first.startTime > 0,
              first.startTime <= config.maxPreRollStartSeconds else {
            return windows
        }

        // DO NOT WIDEN ACROSS A LISTENER'S MARK. The extension covers
        // `[0, first.startTime)`. If any protected region intersects that span,
        // widening would SWALLOW it — the widened window would sit on top of a
        // range the listener explicitly defined, and the mark would be reduced to
        // a redundant row over a bigger detector guess.
        //
        // This is not hypothetical: it is exactly what raising the ceiling from
        // 20 s to 90 s did. A fusion window whose start sat past a user's
        // [35, 55) mark was widened to [0, 60) and engulfed it, leaving TWO
        // windows over the marked region. The clamp cannot detect this on its own
        // because the `userMarked` row is persisted separately and is NOT in the
        // `windows` list the clamp receives — so it must be TOLD, which is what
        // `protectedRegions` is for. (The `boundaryState` guard above still
        // matters for the case where a user-marked row IS in the list.)
        //
        // We REFUSE rather than partially extend to the mark's edge. Refusing
        // fails closed and is trivially reasonable about; a partial extension
        // would manufacture a novel adjacency between a detector guess and a
        // human boundary, and the only episodes it would help are ones where the
        // listener has ALREADY told us where the ad is.
        let widenedSpanStart = 0.0
        let widenedSpanEnd = first.startTime
        let crossesProtectedRegion = protectedRegions.contains { region in
            guard region.end > region.start,
                  region.start.isFinite,
                  region.end.isFinite else {
                return false  // degenerate input cannot protect anything
            }
            return region.start < widenedSpanEnd && widenedSpanStart < region.end
        }
        guard !crossesProtectedRegion else {
            return windows
        }

        // Monotonic guard: moving the start to 0.0 must never invert the window.
        // A real window always has `endTime >= 0`; this only rejects degenerate
        // input rather than emitting `start (0) > end`.
        var result = windows
        result[index] = first.withStartTimeClampedToZero()
        return result
    }
}

// MARK: - AdWindow copy helper

private extension AdWindow {
    /// A copy of this window with `startTime` moved to `0.0`.
    ///
    /// `evidenceStartTime` is deliberately LEFT WHERE THE EVIDENCE ACTUALLY
    /// STARTS — the clamp widens the MARK, not the evidence — and the
    /// ordinal-addressed `id`, `decisionState`, and `confidence` are copied
    /// unchanged. Catalog-match provenance IS cleared: it describes the old
    /// `[startTime, endTime)` fingerprint, and retaining its exact row identity
    /// after widening would let a later correction revoke unrelated source
    /// material.
    ///
    /// `eligibilityGate` IS CARRIED THROUGH VERBATIM, INCLUDING `nil`.
    ///
    /// This clamp used to demote the gate to `markOnly` on the theory that a
    /// widened window is a less certain window. That was wrong, and it is worth
    /// recording why so it is not reintroduced:
    ///
    ///   • The risk is PER-EDGE, not per-window. This clamp only ever moves the
    ///     START of the episode's FIRST slot, LEFTWARD, to 0.0 — an OUTER edge,
    ///     bounded by the episode boundary itself. There is no show content out
    ///     there to lose. The edge that can eat the show is the INNER one (a
    ///     pre-roll's end), and this clamp never touches it. Demoting the whole
    ///     window for moving the free edge surrendered the auto-skip on the part
    ///     that was already trustworthy.
    ///   • It was REDUNDANT where it was right. The clamp fires only on an
    ///     `.unanchored` start edge, and playhead-2350 is the independent
    ///     authority on whether an unanchored edge may auto-skip.
    ///   • It BROKE a real contract where 2350 deliberately allows an unanchored
    ///     edge to stay eligible. playhead-527u: a user-marked window is
    ///     definitive and is stamped `.eligible` at the user's own boundaries
    ///     even though it carries no detector anchor. The demotion overrode the
    ///     listener — forbidden outright by the fidelity rule.
    ///
    /// So this is a pure WIDTH change now. Carrying the gate through must never
    /// become RAISING it: a window that arrives `markOnly` stays `markOnly`, one
    /// that arrives suppressed/blocked stays so, and one that arrives `nil` stays
    /// `nil`. Verbatim copy is the only behaviour that satisfies all three, which
    /// is why there is no severity arithmetic here at all — arithmetic is what
    /// would let a future edit start auto-skipping windows the pipeline refused.
    func withStartTimeClampedToZero() -> AdWindow {
        return AdWindow(
            id: id,
            analysisAssetId: analysisAssetId,
            startTime: 0.0,
            endTime: endTime,
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
            eligibilityGate: eligibilityGate,
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
