// RediffActivation.swift
// playhead-xsdz.36: the SINGLE switch for the rediff ACTIVATION WIRING — the
// mark-only rung Dan approved 2026-07-17 ("ADVANCE TO MARK-ONLY, HOLD
// AUTO-SKIP") and flag-shipped in Gate 1 (#241, `rediffSlotOwnershipEnabled`
// default-ON). Gate 1 made the PASS live but inert: no A-side capture, no
// B-side provider. This switch turns on the remaining production plumbing:
//
//   1. A-SIDE: played-copy fingerprint capture in `AnalysisJobRunner`
//      (the xsdz.27 branch, previously dead behind
//      `EpisodeFingerprintCapture.captureEnabledByDefault == false` — that
//      constant stays `false` and pinned; the runner now takes an injected
//      flag that `PlayheadRuntime` drives from here).
//   2. B-SIDE: the xsdz.28 `RediffRefetchService` BGTask is registered,
//      scheduled, and wired with production conformers
//      (`RediffRefetchProduction.swift`): store-backed enumerator + recorder
//      (R2 failure-state persistence), and the `RediffBSideConsuming` handoff
//      that stages a rotated B-copy into the `RediffBSideStagingProvider`,
//      re-runs the rediff slot pass via
//      `AdDetectionService.revalidateFromFeatures`, then unstages (the
//      service deletes the file — never-persist-B).
//   3. PROVIDER: the `RediffBSideStagingProvider` is injected into
//      `AdDetectionService` so `computeRediffSlotPass` sees staged B-sides.
//
// OFF (`isEnabledByDefault = false`) is BYTE-IDENTICAL to the pre-activation
// app: no capture branch, no provider injected (the pass no-ops on the nil
// guard), no BGTask registered or scheduled, no store writes.
//
// AUTO-SKIP remains held: activation only produces width MARKS (banners) —
// `SkipEligibilityGate` / veto masks are untouched by this switch.

import Foundation

enum RediffActivation {

    /// THE activation switch (playhead-xsdz.36). `true` = mark-only rediff
    /// activation wiring is live (capture + re-fetch + provider). `false` =
    /// byte-identical to the pre-activation app.
    static let isEnabledByDefault = true

    /// Upper bound on episode duration for the A-side chroma capture. The
    /// capture's transient cost is ~159 MB of 11025 Hz PCM per decoded hour
    /// (see `EpisodeFingerprintCapture.captureAndPersist`); beyond ~3 h the
    /// transient risks jetsam on top of the pipeline's own peak.
    ///
    /// COVERAGE CONSEQUENCE (R4): skipping capture forfeits the ENTIRE
    /// rediff lane for the episode — not just the chroma fallback. Re-fetch
    /// candidacy is keyed on the captured A-side stream
    /// (`AnalysisStore.fetchRediffCandidateSeeds` selects only assets with a
    /// current-version `episode_fingerprints` row), so an over-cap episode is
    /// never enumerated for B-side re-fetch and the byte-first differ
    /// (xsdz.57) never receives a B-copy for it, even though that differ
    /// itself needs no chroma A-side. Admitting over-cap episodes would
    /// require a durable no-capture candidacy marker (a deliberate
    /// persistence-design addition — deferred, see the R4 review note on
    /// playhead-xsdz.36).
    static let maxASideCaptureDurationSeconds: TimeInterval = 3 * 60 * 60

    /// Matching bound for the B-side PCM decode in the staging provider's
    /// chroma-fallback path (`refetchedBSideMono16kHz`); the byte-primary
    /// path is unaffected by this cap.
    static let maxBSideDecodeDurationSeconds: TimeInterval = maxASideCaptureDurationSeconds

    /// playhead-xsdz.36.2 (k-way): the number of DISTINCT-persona B-side
    /// re-fetches the PRODUCTION sweep performs per rotated candidate.
    ///
    /// **Deliberately 1 — the single-fetch status quo.** k-way (K≥3) drives
    /// per-slot divergence up (a pod one fetch-pair misses is recovered from
    /// another persona's distinct stitch) but MULTIPLIES the re-fetch bandwidth
    /// on the WiFi+charging BGTask: ~54 MB × K per rotated episode
    /// (~1.1 GB/library-week at K=1, so ~3.3 GB at K=3). Raising K is a data
    /// go/no-go that belongs to the SEPARATE xsdz.36 rollout, NOT this bead — the
    /// k-way MECHANISM ships here config-gated, defaulting to today's bandwidth.
    ///
    /// To activate k-way in production, flip THIS ONE constant (e.g. to `3` for
    /// the iPhone+Mac core plus Overcast). K is capped at the curated persona
    /// bank size (4); see `RediffFetchPersona.kWayPersonas`.
    static let productionKWayFetchCount = 1

    // MARK: - playhead-xsdz.36.4 (day-0 / immediate play-time rediff)

    /// THE day-0 switch (playhead-xsdz.36.4). `true` = the PLAY-TIME trigger
    /// (`DayZeroRediffTrigger`) kicks off an IMMEDIATE k-way rediff for the
    /// just-started episode, so a drop-day listener gets DAI width marks on
    /// FIRST listen instead of waiting for the lagged ≥24h BGTask sweep.
    /// `false` = the trigger site is INERT: no play-time re-fetch is ever
    /// started and no power/network signal is even read — byte-identical to the
    /// lagged-only app.
    ///
    /// DELIBERATELY `false`. A day-0 fetch is a full ~54 MB × K second download
    /// AT PLAY TIME, so flipping it on is a bandwidth/battery go/no-go that
    /// belongs to the SEPARATE xsdz.36 rollout, NOT this bead — the mechanism
    /// ships here gated OFF. Even when on it only runs on WiFi + (charging OR a
    /// user "deep-scan" opt-in), never on cellular or unplugged-without-opt-in;
    /// see `DayZeroRediffGate`. Auto-skip stays held — day-0 is mark-only, on the
    /// SAME `RediffSlotOwnership` marks path as the lagged sweep.
    static let dayZeroEnabledByDefault = false

    /// playhead-xsdz.36.4: the k-way fetch count the DAY-0 trigger uses,
    /// INDEPENDENT of `productionKWayFetchCount` (which governs the lagged
    /// BGTask sweep and stays 1). Day-0 is a single deliberate, gated, immediate
    /// probe, so it draws the iPhone→Mac→Overcast divergence core (3) to reveal
    /// client-PINNED DAI (AdsWizz/ART19) on first listen — the very category a
    /// single-persona probe misses. This bandwidth lives ENTIRELY behind the OFF
    /// `dayZeroEnabledByDefault` flag, so it never perturbs the lagged path's
    /// single-fetch default. Capped at the curated persona bank size (4) by
    /// `RediffFetchPersona.kWayPersonas`.
    static let dayZeroKWayFetchCount = 3
}
