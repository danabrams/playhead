// PreRollStartClamp.swift
// playhead-xsdz.66: the pre-roll start-at-zero clamp — a deterministic DAI
// WIDTH win for the episode's first ad slot.
//
// WHY
// ---
// A pre-roll ad begins at 0:00, but the detector routinely UNDER-measures its
// first few seconds: the ASR / transcript pipeline has a cold-start ramp, so the
// anchored presence core (and therefore the marked pre-roll) starts a few seconds
// late. A pre-roll's start edge is "free" at 0:00 — there is no editorial content
// before it to clip — so when the episode's FIRST ad slot lands in the pre-roll
// zone, its start edge is extended to exactly 0.0. This recovers the width the
// cold-start ramp lost (measured pre-roll width coverage ~57% → ~80%).
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
        /// Default `20.0`. A pre-roll begins at 0:00; the detector's cold-start
        /// miss (ASR warm-up + any intro sting) can push the detected start to
        /// roughly the low-teens of seconds, so 20 s covers the typical miss with
        /// margin. It stays far below any plausible mid-roll (the earliest
        /// mid-rolls land minutes in), so the pre-roll-vs-mid-roll separation is
        /// clean and the clamp cannot swallow a very-early mid-roll.
        var maxPreRollStartSeconds: Double

        static let `default` = Configuration(maxPreRollStartSeconds: 20.0)

        init(maxPreRollStartSeconds: Double = 20.0) {
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
        config: Configuration = .default
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
    /// unchanged. The gate is capped to mark-only because the newly added prefix
    /// has no classifier authority. Catalog-match provenance is also cleared: it
    /// describes the old `[startTime, endTime)` fingerprint, and retaining its
    /// exact row identity after widening would let a later correction revoke
    /// unrelated source material.
    func withStartTimeClampedToZero() -> AdWindow {
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
