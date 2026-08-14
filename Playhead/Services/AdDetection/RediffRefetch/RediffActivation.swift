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
    /// ACTIVATED `true` (playhead-xsdz.36, 2026-07-23). The bandwidth/battery
    /// go/no-go was cleared by the minutes-apart corpus measurement: varied
    /// personas + 9s6q segment recovery turn the two dominant client-PINNED
    /// stacks from 0 → real day-0 width (Conan 211 s; Fresh Air 203 s,
    /// corroborated across Mac+Overcast at matching A-times). A day-0 fetch is
    /// ~54 MB × K at PLAY TIME, so it only runs on a permitted transport +
    /// (charging OR a user "deep-scan" opt-in); see `DayZeroTransportPolicy`
    /// (playhead-4dqe moved the WiFi-vs-cellular leg to a user setting and
    /// added the Low Data Mode override and the daily byte budget — before
    /// that this gate was hardcoded WiFi-only). Auto-skip stays held — day-0 is
    /// MARK-ONLY, on the
    /// SAME `RediffSlotOwnership` marks path as the lagged sweep (a wrong slot
    /// is a banner, never a skip).
    static let dayZeroEnabledByDefault = true

    /// playhead-xsdz.36.4 / playhead-9s6q (FIX B): the k-way fetch count the
    /// DAY-0 trigger uses, INDEPENDENT of `productionKWayFetchCount` (which
    /// governs the lagged BGTask sweep and stays 1). Day-0 is a single
    /// deliberate, gated, immediate probe.
    ///
    /// **2 (playhead-9s6q FIX B), down from 3.** The played A-side copy is
    /// downloaded under a fixed request context (`RediffFetchPersona.download`).
    /// On a client-PINNED show (AdsWizz/ART19) a B-fetch reusing THAT persona
    /// returns a byte-IDENTICAL body — 0 divergent slots, a wasted ~54 MB fetch.
    /// The former K=3 drew `[iPhone, Mac, Overcast]`, whose FIRST persona
    /// collided with the download. Day-0 now stages K=2 VARIED personas
    /// GUARANTEED distinct from the download UA
    /// (`RediffFetchPersona.kWayPersonasDistinct(from:count:)` → `[Mac,
    /// Overcast]`): two real divergence draws, no wasted collision fetch
    /// (~108 MB/play). Still ≥ `RediffSlotOwnership.dayZeroMinKWayBCopies` (2),
    /// the collision-recovery floor. This bandwidth lives ENTIRELY behind the
    /// OFF `dayZeroEnabledByDefault` flag, so it never perturbs the lagged
    /// path's single-fetch default. Capped at the distinct-persona count by
    /// `kWayPersonasDistinct`.
    static let dayZeroKWayFetchCount = 2

    /// playhead-4dqe: the SHIPPING DEFAULT for the day-0 transport setting Dan
    /// moved out of code on 2026-08-01 ("wifi vs 5g should be a user setting,
    /// most people have unlimited bandwidth").
    ///
    /// **`false` — WiFi only by default**, with the toggle surfaced plainly in
    /// Settings. Dan is right about the population; the reason the default goes
    /// the other way is that the cost of a wrong default is ASYMMETRIC. A
    /// metered user who never finds the toggle silently loses ~130 MB per
    /// episode to preparation they did not ask for — the kind of thing that
    /// earns an App Store data-usage complaint. A user with unlimited data
    /// flips it once, costlessly, and never thinks about it again.
    ///
    /// This is only the DEFAULT: the live value is the user's preference
    /// (`UserPreferencesSnapshot.dayZeroAllowsCellular`), and iOS Low Data Mode
    /// overrides both (see `DayZeroTransportPolicy`).
    static let dayZeroAllowsCellularByDefault = false

    // MARK: - playhead-9s6q FIX A (non-monotonic segment recovery)

    /// THE non-monotonic-recovery switch (playhead-9s6q FIX A). `false`
    /// (DEFAULT) = the byte gate REJECTS a non-monotonic alignment wholesale, as
    /// it always has — byte-for-byte identical to the pre-9s6q lagged/production
    /// path. `true` = the byte gate RECOVERS the divergent slots from the
    /// aligner's monotonic-SEGMENT partition
    /// (`RediffSlotOwnership.gateAndDiffBytes(recoverNonMonotonicSegments:)`),
    /// so a high-coverage fetch whose multi-break chain went non-monotonic
    /// (Fresh Air-class: real rotated ads of differing lengths) yields its ad
    /// slots instead of nothing.
    ///
    /// ACTIVATED `true` (playhead-xsdz.36, 2026-07-23). The width-oracle
    /// correctness go/no-go was cleared: on the minutes-apart corpus the
    /// aligner's monotonic-SEGMENT partition recovers Fresh Air's 3 rotated-ad
    /// slots (≈203 s) that the strict wholesale reject discarded, and the slots
    /// are corroborated across the Mac and Overcast personas at matching A-times
    /// (real ad replacements, not fabrication). Only the DAY-0 byte-exact mint
    /// path (`AdDetectionService.mintByteExactDayZeroMarks`) reads this flag; the
    /// LAGGED sweep still passes `false` unconditionally and stays on the strict
    /// wholesale-reject behavior — this activation does NOT touch the lagged
    /// width oracle, so there is no lagged false-widening exposure.
    static let nonMonotonicSegmentRecoveryEnabled = true

    // MARK: - playhead-qs0d (day-0 byte-exact AUTO-SKIP promotion)

    /// THE day-0 auto-skip switch. `true` = a day-0 mark minted from a
    /// STRICT monotonic-clean byte-exact diff is persisted
    /// `eligibilityGate = .eligible` instead of `.markOnly`, so the
    /// orchestrator may auto-skip it. `false` = the xsdz.36.4 mark-only
    /// behavior, byte-for-byte.
    ///
    /// ACTIVATED `true` (playhead-qs0d, 2026-07-31, Dan: "let's turn on rediff
    /// for the next build"). The evidence is one episode on the owner's clean
    /// install, same afternoon: the two `dayZeroRediffByteExact` windows
    /// (confidence 1.00, OUTER edges) were 2-for-2 CORRECT and skipped
    /// NOTHING, while three `segmentAggregated` windows (confidence 0.40–0.42,
    /// INNER edges) were 0-for-3 and skipped 210 s of show. The pipeline was
    /// skipping exactly the wrong population.
    ///
    /// SCOPE — three things this flag does NOT do:
    ///   • It does not touch the per-edge ANCHOR stamp. Strict byte-exact
    ///     slots record `.rediffByteExact` on both edges regardless of this
    ///     flag: that is a correctness fix (the edges really were set by the
    ///     byte differ), not a permission grant, and `unanchored` there meant
    ///     "nobody wrote an anchor", never "the boundary is unknown".
    ///   • It does not, ON ITS OWN, promote playhead-9s6q SEGMENT-RECOVERED
    ///     (non-monotonic) slots — that is
    ///     `dayZeroSegmentRecoveredAutoSkipEnabled` below, which playhead-pyq7
    ///     added and which this switch still gates (it is the MASTER: with this
    ///     `false` nothing day-0 auto-skips, whatever the recovered switch says).
    ///   • It does not enable auto-skip for any other producer.
    static let dayZeroByteExactAutoSkipEnabled = true

    // MARK: - playhead-pyq7 (day-0 SEGMENT-RECOVERED auto-skip promotion)

    /// THE segment-recovered promotion switch. `true` = a day-0 mark minted
    /// from a playhead-9s6q SEGMENT-RECOVERED (non-monotonic) byte diff is
    /// stamped `.rediffByteExact` on both edges and — under the
    /// `dayZeroByteExactAutoSkipEnabled` master above — persisted
    /// `eligibilityGate = .eligible`, exactly like a strict slot. `false` =
    /// the playhead-qs0d behaviour, byte-for-byte: recovered slots keep the
    /// conservative `.unanchored` pair and `.markOnly`.
    ///
    /// ACTIVATED `true` (playhead-pyq7, 2026-08-14, Dan's call after the
    /// read-the-device-then-promote round). Two measurements, one controlled
    /// and one from the field, and the second is what made the first
    /// actionable:
    ///
    ///  • CONTROLLED, one tree, one build of each (BEFORE = the pre-playhead-3zxd
    ///    accept rule pinned back in as a mutant; AFTER = shipped main). Over the
    ///    NON-MONOTONIC population this arm exists for: emitted slots 20 → 13,
    ///    PHANTOMS (slots containing zero gold ad seconds) **7 → 0**, total show
    ///    eaten 2449.9965 s → **0.0000 s**, widest emitted slot 469.9949 s →
    ///    29.9886 s, and slots matching a real gold ad 13 → 13 — the fix cost NO
    ///    recall. The worst shape `299.99..629.97` (a 30 s ad fused to 299.99 s
    ///    of show) became `599.98..629.97`: the ad and only the ad.
    ///  • EDGES, over all 38 emitted slots: INNER start p50 +0.0002 s, p95/max
    ///    +0.0003 s; INNER end p50/p95/max +0.0000 s; OUTER start all +0.0000 s.
    ///    The sign is always the SAFE direction (start late, end early) and the
    ///    residual is the 4-byte shared CBR frame header, 4/417 of a 26 ms
    ///    frame. `AutoSkipEdgePadding`'s rediff margins (0.50 s in at the start,
    ///    0.75 s off the end) are three orders of magnitude larger, so the qs0d
    ///    derivation covers this population with room to spare — stated rather
    ///    than assumed, because those margins were derived on monotonic-clean
    ///    chains and this is a different arm.
    ///  • FIELD, `db-pull10` (build 4f6bd5d3, 5 assets, 13 day-0 marks): three
    ///    assets took the recovery arm with `lastRunsAOverlapping` 1/2/1 and
    ///    `lastOverlapSecondsRecovered` 60.3 / 82.9 / **701.0** s — so the defect
    ///    class was live on real audio and the fix engaged there — while
    ///    `lastAlignedSecondsInSlots` and `lastMaxAlignedSecondsInSlot` read
    ///    **0.0000 everywhere**: not one show second inside any emitted slot.
    ///
    /// WHAT THE PROMOTION IS WORTH, and why it is not a nicety: 9 of those 13
    /// marks (69 %) were segment-recovered, and all 9 are on the three Conan
    /// episodes Dan actually listens to. Zero of three day-0 marks on the show
    /// he opened were auto-skippable.
    ///
    /// WHAT IT DOES NOT DO. It is not a new door into auto-skip: playhead-2350's
    /// extent rule, `qs0d`'s targeted padding activation and `ynmk`'s
    /// confirmation semantics are untouched and all three key off the anchor
    /// PAIR — this switch supplies that pair honestly rather than bypassing
    /// anything. It does not retro-fit rows already on disk (the mint is
    /// idempotent), and it does not widen the playhead-ug9m supersede guard,
    /// which still demands a STRICT re-mint.
    static let dayZeroSegmentRecoveredAutoSkipEnabled = true
}
