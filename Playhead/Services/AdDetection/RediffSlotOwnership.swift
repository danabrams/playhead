// RediffSlotOwnership.swift
// playhead-xsdz.29: the OFFLINE rediff width-oracle integration — the pure,
// deterministic core that turns a stored played-copy fingerprint stream (A) and
// a re-fetched B-side copy into played-timeline DAI ad slots, then into
// `SpliceSlotCandidate`s the REUSED-UNCHANGED `SpliceSlotDispositionEngine`
// consumes. The service wires this behind the flag family (default OFF) and
// materializes accepted slots via `SpliceSlotRewriter.apply(provenance: .rediffSlot)`.
//
// CONTRACT (strategy panel 2026-07-07): rediff is the SOLE production width
// SETTER. A decoded span WITHOUT an overlapping rediff slot falls through to
// STATUS-QUO transcript width (candidate `.slot == nil` → `.noSlot` → span
// unchanged) — it NEVER consults the acoustic `SpliceSlotResolver`. The acoustic
// channel is shadow/eval-only. This engine therefore never calls the resolver;
// it only produces candidates the shared disposition engine can grade.
//
// DOUBLE-GATE (design input 3, xsdz.27): a stored A-side stream is trusted only
// when BOTH (a) its `algorithmVersion` equals the current
// `ChromaFingerprinter.algorithmVersion` (defensive re-check; the store's fetch
// already enforces this, but the record carries the field and B is fingerprinted
// at the CURRENT version, so we re-assert it here), AND (b) its persisted
// `sourceAudioIdentity` equals the asset's CURRENT `assetFingerprint`. Without
// (b), an assetId reused for a re-download of DIFFERENT audio would return a
// version-matching but audio-MISMATCHED stream and silently misalign.
//
// EXTRACTOR IDENTITY (design input 2, xsdz.27): the B-side is fingerprinted via
// `EpisodeFingerprintCapture.fingerprints(mono16kHz:)` — the EXACT same
// resample(16k→11025 linear)→ChromaFingerprint path the A-side was captured
// with. The `(resampler + fingerprinter)` pair is ONE versioned unit; comparing
// A and B fingerprinted by different extractors is meaningless.
//
// RE-ENCODE GUARD (Megaphone/mgln.ai, spike §7): a per-stitch re-encoding CDN
// makes A and B fail to fingerprint-align anywhere (`alignedFractionB` collapses
// toward 0). Slots are trusted only when `alignedFractionB >= minAlignedFractionB`
// (0.5). Below that the whole rediff result is discarded → status-quo width.
//
// CONSUMER-SIDE CLEANUP (spike §4/§6.3): fingerprint dropouts around splices
// fragment slot edges (nikki-glaser: 18 A-side fragments), and an alignment
// breakdown can produce an implausibly long "slot" (casefile: a 29-minute span).
// So played slots are (1) FRAGMENT-MERGED across gaps ≤ `fragmentMergeGapSeconds`
// (3 s) and (2) DURATION-CAPPED: a slot longer than `maxSlotSeconds` (8 min) is
// an alignment breakdown, not an ad, and is dropped.
//
// PURITY: static functions over value types, `Foundation` only, deterministic,
// no I/O and no actor hops. The store read (A-side), the negative-bank verdict
// table, and the B-side fetch all live in the service; this engine takes them as
// inputs so its whole surface is offline-testable.

import Foundation

enum RediffSlotOwnership {

    // MARK: - Configuration

    struct Configuration: Sendable, Equatable {
        /// Minimum `alignedFractionB` before any slot is trusted (re-encode
        /// guard). Below this the rediff result is discarded wholesale.
        var minAlignedFractionB: Double
        /// Minimum ad length fed to the differ's `minGapFps` threshold.
        var minAdSeconds: Double
        /// Adjacent played slots separated by a run gap ≤ this are merged into
        /// one (fragment-dropout repair). `0` disables merging.
        var fragmentMergeGapSeconds: Double
        /// A played slot longer than this is an alignment breakdown, not an ad,
        /// and is dropped (sanity cap).
        var maxSlotSeconds: Double
        /// Minimum fraction of a decoded span's presence CORE that a rediff slot
        /// must cover to own that span's width. Mirrors
        /// `SpliceSlotResolver.Configuration.minCoreCoverage` (0.8): a slot that
        /// owns WIDTH must still CONTAIN the presence it widens. A rediff slot
        /// that covers less of the core than this leaves the span at status-quo
        /// width. (Undersized cores normally sit fully INSIDE the true-width
        /// rediff slot → coverage ≈ 1.0; this gate only rejects a core that
        /// sticks OUT of the slot — a mis-association.)
        var minCoreCoverage: Double

        static let `default` = Configuration(
            minAlignedFractionB: 0.5,
            minAdSeconds: 5.0,
            fragmentMergeGapSeconds: 3.0,
            maxSlotSeconds: 480.0,
            minCoreCoverage: 0.8
        )

        init(
            minAlignedFractionB: Double = 0.5,
            minAdSeconds: Double = 5.0,
            fragmentMergeGapSeconds: Double = 3.0,
            maxSlotSeconds: Double = 480.0,
            minCoreCoverage: Double = 0.8
        ) {
            self.minAlignedFractionB = minAlignedFractionB
            self.minAdSeconds = minAdSeconds
            self.fragmentMergeGapSeconds = fragmentMergeGapSeconds
            self.maxSlotSeconds = maxSlotSeconds
            self.minCoreCoverage = minCoreCoverage
        }
    }

    // MARK: - Gate outcome

    /// Why the A/B comparison did or did not yield trustworthy slots. Every
    /// rejection reason is distinct so the service breadcrumb / eval can size
    /// each failure mode.
    enum GateOutcome: Sendable, Equatable {
        /// Stored A-side `algorithmVersion` ≠ current — uncomparable.
        case rejectedStaleVersion(stored: UInt32, current: UInt32)
        /// Stored `sourceAudioIdentity` ≠ the asset's current `assetFingerprint`
        /// — the stored stream is for DIFFERENT audio (assetId reuse).
        case rejectedAudioIdentityMismatch(stored: String, current: String)
        /// `alignedFractionB` below the re-encode floor — discard wholesale.
        case rejectedLowAlignedFraction(Double)
        /// Accepted: the diff plus the cleaned played-timeline slots.
        case accepted(Acceptance)
    }

    /// A gate-accepted rediff comparison.
    struct Acceptance: Sendable, Equatable {
        /// The raw differ result (diagnostics / eval).
        let rediffResult: RediffDiffer.Result
        /// `alignedFractionB` (≥ the floor, by construction).
        let alignedFractionB: Double
        /// Played-timeline (A-side) DAI ad slots AFTER fragment-merge and
        /// duration-cap. These are the width oracle's output.
        let playedSlots: [PlayedSlot]
    }

    /// One played-timeline DAI ad slot: seconds in the user's PLAYED copy plus
    /// the flanking aligned-run lengths that set edge confidence.
    struct PlayedSlot: Sendable, Equatable {
        let startSeconds: Double
        let endSeconds: Double
        let leftRunSeconds: Double
        let rightRunSeconds: Double

        var durationSeconds: Double { endSeconds - startSeconds }
    }

    // MARK: - Gate + diff (pure)

    /// Apply the double-gate, fingerprint the B-side via the EXACT xsdz.27 path,
    /// run the differ, apply the re-encode guard, then fragment-merge +
    /// duration-cap the played slots.
    ///
    /// - Parameters:
    ///   - storedASide: the persisted played-copy fingerprint record (A).
    ///   - refetchedBSideSamples16kHz: the re-fetched copy as mono 16 kHz PCM
    ///     (B). Fingerprinted here — NEVER pre-fingerprinted by the caller — so
    ///     the `(resampler+fingerprinter)` versioned unit is applied identically
    ///     to both sides.
    ///   - currentAssetFingerprint: the asset's CURRENT `assetFingerprint` for
    ///     the sourceAudioIdentity gate.
    static func gateAndDiff(
        storedASide: EpisodeFingerprintRecord,
        refetchedBSideSamples16kHz: [Float],
        currentAssetFingerprint: String,
        config: Configuration = .default
    ) -> GateOutcome {
        // GATE (a): version. Defensive — the store already enforces this, but B
        // is fingerprinted at the CURRENT version, so a stale A is uncomparable.
        let current = ChromaFingerprinter.algorithmVersion
        guard storedASide.algorithmVersion == current else {
            return .rejectedStaleVersion(stored: storedASide.algorithmVersion, current: current)
        }
        // GATE (b): source-audio identity. An assetId reused for re-downloaded
        // (different) audio returns a version-matching but audio-MISMATCHED
        // stream; reject it before trusting it as the rediff A-side.
        guard storedASide.sourceAudioIdentity == currentAssetFingerprint else {
            return .rejectedAudioIdentityMismatch(
                stored: storedASide.sourceAudioIdentity,
                current: currentAssetFingerprint
            )
        }

        // Fingerprint B via the EXACT xsdz.27 extractor (resample→ChromaFingerprint).
        let fpB = EpisodeFingerprintCapture.fingerprints(mono16kHz: refetchedBSideSamples16kHz)

        let result = RediffDiffer.rediff(
            fingerprintA: storedASide.fingerprints,
            secondsPerFpA: storedASide.secondsPerFingerprint,
            fingerprintB: fpB,
            secondsPerFpB: ChromaFingerprinter.secondsPerFingerprint,
            minAdSeconds: config.minAdSeconds
        )

        // RE-ENCODE GUARD.
        guard result.alignedFractionB >= config.minAlignedFractionB else {
            return .rejectedLowAlignedFraction(result.alignedFractionB)
        }

        let cleaned = cleanedPlayedSlots(from: result.slotsA, config: config)
        return .accepted(Acceptance(
            rediffResult: result,
            alignedFractionB: result.alignedFractionB,
            playedSlots: cleaned
        ))
    }

    /// Fragment-merge (gaps ≤ `fragmentMergeGapSeconds`) then duration-cap the
    /// differ's A-side slots into clean played slots. Pure; exposed for tests.
    static func cleanedPlayedSlots(
        from slotsA: [RediffDiffer.Slot],
        config: Configuration = .default
    ) -> [PlayedSlot] {
        mergedAndCapped(
            slotsA.map { PlayedSlot(
                startSeconds: $0.startSeconds,
                endSeconds: $0.endSeconds,
                leftRunSeconds: $0.leftRunSeconds,
                rightRunSeconds: $0.rightRunSeconds
            ) },
            config: config
        )
    }

    /// The shared fragment-merge + duration-cap over played slots, applied
    /// IDENTICALLY to chroma-derived (`cleanedPlayedSlots`) and byte-derived
    /// (`gateAndDiffBytes`) slots so both differs feed the downstream
    /// candidate/veto/disposition machinery the same slot shape.
    static func mergedAndCapped(
        _ slots: [PlayedSlot],
        config: Configuration = .default
    ) -> [PlayedSlot] {
        guard !slots.isEmpty else { return [] }
        // Sort by start (differ output is already ordered, but be defensive).
        let sorted = slots.sorted { ($0.startSeconds, $0.endSeconds) < ($1.startSeconds, $1.endSeconds) }
        var merged: [PlayedSlot] = [sorted[0]]
        for slot in sorted.dropFirst() {
            let last = merged[merged.count - 1]
            // Merge when the inter-slot gap is a short aligned-run dropout. The
            // OUTER flanks (last.left, slot.right) carry the real confidence; the
            // tiny joined run between fragments is not a slot boundary.
            if slot.startSeconds - last.endSeconds <= config.fragmentMergeGapSeconds {
                merged[merged.count - 1] = PlayedSlot(
                    startSeconds: last.startSeconds,
                    endSeconds: max(last.endSeconds, slot.endSeconds),
                    leftRunSeconds: last.leftRunSeconds,
                    rightRunSeconds: slot.rightRunSeconds
                )
            } else {
                merged.append(PlayedSlot(
                    startSeconds: slot.startSeconds,
                    endSeconds: slot.endSeconds,
                    leftRunSeconds: slot.leftRunSeconds,
                    rightRunSeconds: slot.rightRunSeconds
                ))
            }
        }
        // DURATION CAP: drop alignment-breakdown giants OUTRIGHT (intentionally
        // NOT reverting to the pre-merge fragments — a > maxSlotSeconds region is
        // an alignment breakdown, not a real ad, so the span falls to status-quo
        // width rather than being widened to a suspect fragment).
        return merged.filter { $0.durationSeconds <= config.maxSlotSeconds }
    }

    // MARK: - K-way union (playhead-xsdz.36.2)

    /// UNION the played-slot lists from K pairwise byte diffs (A vs each of the
    /// K distinct-persona B-sides) into ONE slot set. Because a single
    /// fetch-PAIR can MISS an ad pod when both draws land the same fill on a
    /// low-entropy slot, unioning the divergent regions across the fetch set
    /// recovers pods any one pair misses (the "B vs C misses / B+C+D recovers"
    /// case).
    ///
    /// BACKWARD-COMPAT: a single list (K=1) is returned UNCHANGED — byte-identical
    /// to the pre-k-way single-fetch differ output, which is already cleaned by
    /// `gateAndDiffBytes`. Multiple lists are concatenated and re-cleaned by the
    /// SAME `mergedAndCapped` the 2-way path uses, so overlapping detections of
    /// the same pod collapse to one slot and the union carries the identical slot
    /// shape downstream (candidate → veto → disposition). Each input list is
    /// assumed already `mergedAndCapped` (as `gateAndDiffBytes` returns them);
    /// re-cleaning is idempotent for a single list and correct for the union.
    static func unionedPlayedSlots(
        _ perBSideSlots: [[PlayedSlot]],
        config: Configuration = .default
    ) -> [PlayedSlot] {
        // K=1 (or a single accepted B) → verbatim, no re-clean: the crisp
        // "reduces to today's EXACT single-fetch behavior" guarantee.
        guard perBSideSlots.count > 1 else { return perBSideSlots.first ?? [] }
        return mergedAndCapped(perBSideSlots.flatMap { $0 }, config: config)
    }

    // MARK: - Strict (monotonic-clean) classification (playhead-qs0d)

    /// Which slots of a k-way `unioned` set are STRICT byte-exact — i.e. their
    /// geometry is reproduced EXACTLY by the union of the monotonic-clean
    /// personas alone. Returns a mask index-aligned with `unioned`.
    ///
    /// Why this exists. The day-0 mint unions per-persona slot lists that come
    /// from two different acceptance arms: the strict byte gate (a chain that
    /// dropped nothing, so every edge is a proven A-timeline byte divergence)
    /// and the playhead-9s6q segment-recovery arm (a chain that DID drop runs
    /// and was rescued by the monotonic-segment partition). Only the first has
    /// earned an auto-skip anchor. Because `unionedPlayedSlots` MERGES across
    /// personas, "which arm produced this slot" is not a per-list property —
    /// a recovered persona can widen, or bridge, a slot a strict persona found.
    /// Recomputing the union over the strict subset and demanding an EXACT
    /// geometry match is the conservative answer to that: any contribution from
    /// a recovered persona moves an edge (or merges two slots) and the match
    /// fails, so the slot falls back to mark-only.
    ///
    /// Exact equality is deliberate, not sloppy. Both sides are the identical
    /// `mergedAndCapped` arithmetic (`max`, comparison, and verbatim
    /// pass-through — no accumulation) over overlapping inputs, so an
    /// untouched slot agrees bit-for-bit. A tolerance would only ever admit a
    /// slot a recovered persona DID move.
    ///
    /// Pure and total. An empty `strictPerBSideSlots` yields an all-false mask
    /// — the correct answer when every persona needed recovery.
    ///
    /// playhead-pyq7 NOTE ON WHAT `false` NOW COSTS. Before pyq7 a `false` here
    /// meant `.unanchored` + `.markOnly`. It no longer decides that on its own —
    /// see `dayZeroSlotIsSkipGrade` — but the mask itself is UNCHANGED and still
    /// answers exactly one question: *did a recovered persona touch this
    /// geometry?*, and it is still what
    /// `RediffDayZeroMintOutcome.strictMarkCount` counts.
    ///
    /// playhead-c7ef: it is NO LONGER the input to the ug9m supersede guard.
    /// That guard now reads SKIP GRADE plus a geometric containment rule
    /// (`DayZeroMarkCensus.reMintMayReplace`), because `strict` names an
    /// acceptance ARM and the question the guard has to answer — "is the row
    /// this replaces strictly worse, and does the swap cut anything that was not
    /// already marked?" — is not a question about arms.
    static func strictByteExactMask(
        unioned: [PlayedSlot],
        strictPerBSideSlots: [[PlayedSlot]],
        config: Configuration = .default
    ) -> [Bool] {
        let strictUnioned = unionedPlayedSlots(strictPerBSideSlots, config: config)
        return unioned.map { slot in
            strictUnioned.contains {
                $0.startSeconds == slot.startSeconds && $0.endSeconds == slot.endSeconds
            }
        }
    }

    // MARK: - Skip grade (playhead-pyq7)

    /// Has this day-0 slot earned `.rediffByteExact` anchors and, under the
    /// `RediffActivation.dayZeroByteExactAutoSkipEnabled` master switch,
    /// auto-skip eligibility?
    ///
    /// Two arms, because `strictByteExactMask` is a TOTAL classification of the
    /// unioned slots: `strict` is the monotonic-clean one, and `!strict` is
    /// exactly "a playhead-9s6q segment-recovered persona created or moved this
    /// geometry". So the second parameter names the population it governs
    /// rather than acting as a global override, and the two arms are spelled
    /// out rather than collapsed to `strict || enabled` for the same reason.
    ///
    /// A FUNCTION RATHER THAN AN EXPRESSION AT THE MINT, on purpose. The flag it
    /// reads is a compile-time `static let`, so the only way a test can exercise
    /// the OFF state is to hold the flag as a parameter. `false` here is not a
    /// dead branch — it is the behaviour a rollback restores, and it is pinned
    /// exhaustively in `RediffDayZeroSkipGradeTests`.
    static func dayZeroSlotIsSkipGrade(
        strict: Bool,
        segmentRecoveredPromotionEnabled: Bool
    ) -> Bool {
        if strict { return true }
        return segmentRecoveredPromotionEnabled
    }

    // MARK: - Day-0 k-way minimum (playhead-xsdz.36.4 / playhead-wybg)

    /// Minimum number of DISTINCT-persona B-copies a day-0 byte-exact probe must
    /// have staged before it attempts a mint. `2` — this is NOT a corroboration
    /// quorum. The day-0 mint UNIONs the per-persona byte-exact slots via
    /// `unionedPlayedSlots` (quorum = 1: a slot mints if ANY one persona's diff
    /// reveals it), exactly like the lagged path. This constant is instead a
    /// COLLISION-RECOVERY floor: on a client-PINNED show (Conan/AdsWizz) a single
    /// re-fetch can land the SAME stitch as the played copy (byte-identical → 0
    /// divergent slots → reveals nothing), so day-0 requires ≥2 distinct-persona
    /// B-copies to give a divergence a chance. The per-persona byte gate
    /// (`gateAndDiffBytes`: min-run-bytes, monotonic-clean, chainedFractionB ≥
    /// floor) is the PRECISION guard; staging ≥2 personas is the RECALL guard.
    ///
    /// playhead-wybg: the former `kWayRobustPlayedSlots` ≥2-AGREEMENT quorum was
    /// REMOVED. A minutes-apart (realistic day-0 timing) measurement showed that on
    /// pinned shows only ONE persona (e.g. Overcast) diverges and reveals the real
    /// ads (211 s), while the same-persona re-fetch collides (byte-identical); a
    /// ≥2-agreement quorum dropped those ads, defeating the entire k-way
    /// collision-recovery purpose. Union + the byte gate is precise single-fetch,
    /// so day-0 unifies with the lagged union path.
    static let dayZeroMinKWayBCopies = 2

    // MARK: - Byte-path gate (playhead-xsdz.57 — PRIMARY differ)

    /// Why a byte alignment did or did not yield trustworthy slots. EVERY
    /// rejection is a FALLBACK TRIGGER (the service then runs the chroma differ
    /// exactly as pre-xsdz.57), never an error: a re-encoding CDN
    /// (nikki-glaser) legitimately produces near-zero common bytes.
    ///
    /// IDENTITY GATES (why the chroma double-gate does NOT apply here): the
    /// chroma path diffs a PERSISTED fingerprint stream whose staleness the
    /// `algorithmVersion` / `sourceAudioIdentity` gates guard against. The byte
    /// path consumes NO persisted stream — its A input is the asset row's live
    /// audio file, read at diff time — so there is nothing to go stale. The
    /// service instead anchors both file inputs with the regular-unaliased-file
    /// check (bf4a2383 precedent) before any bytes are read.
    enum ByteGateOutcome: Sendable, Equatable {
        /// No chained runs — zero anchors (a wholesale re-encode collapses the
        /// unique-frame anchor set toward 0) or nothing survived min-run.
        case rejectedNoChainedRuns
        /// The chain dropped runs (python `monotonic_clean == false`) — an
        /// out-of-order byte structure the strict slot semantics cannot trust.
        /// playhead-9s6q: with `recoverNonMonotonicSegments` set (day-0 opt-in),
        /// a fetch that would land here is instead segment-recovered; it only
        /// still reaches this case when the segmented recovery finds no
        /// trustworthy slot (low coverage / all sub-ad-width).
        case rejectedNonMonotonic(dropped: Int)
        /// `chainedFractionB` below the re-encode floor (the byte analogue of
        /// the chroma `alignedFractionB` guard, SAME `minAlignedFractionB`).
        case rejectedLowChainedFraction(Double)
        /// Accepted: A-time played slots + scalar diagnostics.
        case accepted(ByteAcceptance)
    }

    /// A gate-accepted byte alignment. A-TIME ONLY (xsdz.28 never-persist-B):
    /// deliberately carries NO `RediffByteAligner.Run` values and NO B-side
    /// byte coordinates — only played-timeline slots and scalar diagnostics —
    /// so no B coordinate can outlive the diff through this surface.
    struct ByteAcceptance: Sendable, Equatable {
        let chainedFractionB: Double
        let runsFound: Int
        let runsChained: Int
        /// Played-timeline (A-side) DAI ad slots AFTER the A-width ≥
        /// `minAdSeconds` filter, fragment-merge, and duration-cap — the SAME
        /// cleaning the chroma acceptance applies. These feed the SAME
        /// `candidates(...)` → `resolveSpan(...)` → disposition flow, so the
        /// xsdz.34 §5 veto gate and all grading apply identically.
        let playedSlots: [PlayedSlot]
        /// playhead-3zxd instrumentation for THIS diff (see `ByteDiagnostics`).
        /// A-time seconds and counts only, so the never-persist-B contract above
        /// is untouched: `foundRunASpans` carries A-timeline spans, never a B
        /// offset, and a run's length is identical in both files.
        let diagnostics: ByteDiagnostics
    }

    /// playhead-3zxd — what one byte diff can tell a device pull about the
    /// phantom-slot defect, WITHOUT gold and without a second detector.
    ///
    /// The three counters the aligner already kept (`runsFound`,
    /// `runsDroppedNonMonotonic`, `segmentedRunsChained`) can say how much
    /// structure was found. None of them can say whether an EMITTED slot is made
    /// of audio the aligner itself proved matched, which is the whole question.
    /// These can, and they are computable on the day-0 path — which mints before
    /// a second has been transcribed — so every episode Dan adds is a real data
    /// point instead of a staged fixture.
    struct ByteDiagnostics: Sendable, Equatable {
        /// Runs `byteRuns` found. VACUITY CONTROL: `0` means every other field
        /// below says nothing. Note that this struct is `.empty` on every
        /// rejection, so at the mint level `0` reads as "no persona was
        /// accepted" rather than as any particular cause — R3 review, and see
        /// `RediffByteMintDiagnostics.runsFound`.
        var runsFound: Int = 0
        /// See `RediffByteAligner.Alignment.segmentedRunsAOverlapping` — the
        /// OPPORTUNITY counter. `> 0` means a pre-playhead-3zxd build could have
        /// emitted a phantom on this diff.
        var runsAOverlapping: Int = 0
        /// See `RediffByteAligner.Alignment.segmentedOverlapSecondsRecovered` —
        /// an UPPER BOUND on the show-seconds a pre-3zxd build would have
        /// reported as an ad here.
        var overlapSecondsRecovered: Double = 0
        /// Σ over the EMITTED slots of the A-seconds a found run covers — the
        /// invariant witness. Zero by construction after playhead-3zxd.
        ///
        /// Measured post `mergedAndCapped` rather than over the aligner's raw
        /// gaps, because a banner the listener can tap is the thing at stake.
        ///
        /// PER-PERSONA, and that pairing is deliberate, not incidental: THIS
        /// diff's slots against THIS diff's runs. Scoring one persona's runs
        /// against the k-way UNION would be wrong by minutes — the union is
        /// quorum-1 (`unionedPlayedSlots`: a slot mints if ANY persona diverged
        /// on it), so a region persona 0 matched while persona 1 diverged is the
        /// collision-recovery this design exists for, not a phantom.
        ///
        /// R5 REVIEW — WHAT THAT PAIRING COSTS, because "the slots as they SHIP"
        /// is what an earlier draft of this note claimed and it is not exactly
        /// true. `mintByteExactDayZeroMarks` runs `unionedPlayedSlots` over the
        /// accepted personas, which re-runs `mergedAndCapped` across their
        /// concatenation; whenever ≥ 2 personas are accepted — the ordinary k-way
        /// shape — the geometry that reaches a listener is that RE-MERGE, and no
        /// persona measured it. So there are two joins, both bounded by
        /// `fragmentMergeGapSeconds` (3 s) and both non-phantom:
        ///
        ///   * WITHIN a persona this number DOES see the join, and the join is
        ///     hard to make: what separates two shippable gaps is a run accepted
        ///     WHOLE, 4.1 s at 128 kbps CBR, joinable only at ≥192 kbps
        ///     (playhead-yzra, filed separately).
        ///   * ACROSS personas it sees nothing at all — the separator there is
        ///     not a run, so nothing stops the join, and the miss is silent.
        ///
        /// Both are ≤ 3 s per join and both UNDER-report, so the number is
        /// optimistic by single-digit seconds and never pessimistic. Read the
        /// MAGNITUDE: seconds is merge; this bead's phantom is minutes.
        var alignedSecondsInSlots: Double = 0
        /// The worst single emitted slot, so a large value cannot hide inside a
        /// sum spread over many slots. Per-persona and pre-re-merge for the same
        /// reason, and with the same ≤ 3 s under-report — see above.
        var maxAlignedSecondsInSlot: Double = 0
        /// A-TIME span of every FOUND run. The raw material: with it a later
        /// question can be re-asked of data already collected, rather than only
        /// the scalars somebody thought to define today. Bounded by the caller
        /// before it is persisted (`RediffByteMintDiagnostics`).
        var foundRunASpans: [TimeRange] = []

        static let empty = ByteDiagnostics()
    }

    /// Measure `playedSlots` against the runs the alignment FOUND.
    ///
    /// `playedSlots` must be the list this alignment actually emitted — passing
    /// slots from a different diff would silently describe the wrong pair, which
    /// is why this is private and called only from the two acceptance sites.
    private static func byteDiagnostics(
        alignment: RediffByteAligner.Alignment,
        playedSlots: [PlayedSlot]
    ) -> ByteDiagnostics {
        var total = 0.0
        var worst = 0.0
        for slot in playedSlots {
            let matched = RediffByteAligner.alignedSeconds(
                in: TimeRange(start: slot.startSeconds, end: slot.endSeconds),
                runASpans: alignment.foundRunASpans
            )
            total += matched
            worst = max(worst, matched)
        }
        return ByteDiagnostics(
            runsFound: alignment.runsFound,
            runsAOverlapping: alignment.segmentedRunsAOverlapping,
            overlapSecondsRecovered: alignment.segmentedOverlapSecondsRecovered,
            alignedSecondsInSlots: total,
            maxAlignedSecondsInSlot: worst,
            foundRunASpans: alignment.foundRunASpans
        )
    }

    /// Gate a byte alignment (the PRIMARY differ) into played slots, mirroring
    /// the python reference's `monotonic_clean` / quality gates:
    ///   • empty chain          → `.rejectedNoChainedRuns` (fallback)
    ///   • dropped runs         → `.rejectedNonMonotonic` (fallback)
    ///   • low chained fraction → `.rejectedLowChainedFraction` (fallback —
    ///     the re-encode-CDN case, byte analogue of the chroma guard)
    /// Accepted slots are filtered to A-width ≥ `minAdSeconds` (the byte
    /// analogue of the chroma differ's `minGapFps` floor — pure-B insertions
    /// have zero A-width and drop out here), then fragment-merged and
    /// duration-capped by the SAME `mergedAndCapped` the chroma path uses.
    ///
    /// playhead-9s6q FIX A (`recoverNonMonotonicSegments`): DEFAULT `false` is
    /// the historical WHOLESALE reject on a non-monotonic chain — byte-identical
    /// to the pre-9s6q lagged/production path. When `true` (the day-0 opt-in),
    /// a non-monotonic alignment is instead RECOVERED via the aligner's
    /// monotonic-SEGMENT partition (`segmentRecoveredByteGate`), preserving every
    /// precision guard. The lagged sweep passes `false`; nothing changes until a
    /// separate corpus go/no-go flips the flag on.
    static func gateAndDiffBytes(
        alignment: RediffByteAligner.Alignment,
        config: Configuration = .default,
        recoverNonMonotonicSegments: Bool = false
    ) -> ByteGateOutcome {
        guard !alignment.chain.isEmpty else {
            return .rejectedNoChainedRuns
        }
        guard alignment.monotonicClean else {
            // playhead-9s6q FIX A: the day-0 opt-in RECOVERS the divergent slots
            // from the monotonic segments; the strict path (default) discards the
            // whole fetch exactly as before.
            guard recoverNonMonotonicSegments else {
                return .rejectedNonMonotonic(dropped: alignment.runsDroppedNonMonotonic)
            }
            return segmentRecoveredByteGate(alignment: alignment, config: config)
        }
        guard alignment.chainedFractionB >= config.minAlignedFractionB else {
            return .rejectedLowChainedFraction(alignment.chainedFractionB)
        }
        let playedSlots = mergedAndCapped(
            alignment.slots
                .filter { $0.aSeconds >= config.minAdSeconds }
                .map { PlayedSlot(
                    startSeconds: $0.aStartSeconds,
                    endSeconds: $0.aEndSeconds,
                    leftRunSeconds: $0.leftFlankSeconds,
                    rightRunSeconds: $0.rightFlankSeconds
                ) },
            config: config
        )
        return .accepted(ByteAcceptance(
            chainedFractionB: alignment.chainedFractionB,
            runsFound: alignment.runsFound,
            runsChained: alignment.chain.count,
            playedSlots: playedSlots,
            diagnostics: byteDiagnostics(alignment: alignment, playedSlots: playedSlots)
        ))
    }

    /// playhead-9s6q FIX A: the non-monotonic RECOVERY arm. Preserves EVERY
    /// precision guard of the strict path, applied to the SEGMENTED coverage:
    ///   • re-encode floor — `segmentedChainedFractionB` (Σ segment run bytes /
    ///     B audio bytes) must clear `minAlignedFractionB`, so a low-coverage
    ///     island (a re-encode) is STILL rejected wholesale (not widened);
    ///   • min-run-bytes — intrinsic at the SOURCE (`byteRuns` emits nothing
    ///     shorter). NOT of an ACCEPTED run: playhead-3zxd's clip can leave a
    ///     run of one byte. See `RediffByteAligner.segmentDivergentSlots` for
    ///     why the fragment-merge bound below survives that anyway;
    ///   • min-ad-width + fragment-merge + duration-cap — the SAME
    ///     `mergedAndCapped` cleaning the strict path applies, so sub-ad and
    ///     alignment-breakdown gaps are dropped.
    /// If nothing survives the guards, this returns the SAME `.rejectedNonMonotonic`
    /// the strict path would (no manufactured acceptance).
    private static func segmentRecoveredByteGate(
        alignment: RediffByteAligner.Alignment,
        config: Configuration
    ) -> ByteGateOutcome {
        guard alignment.segmentedChainedFractionB >= config.minAlignedFractionB else {
            return .rejectedLowChainedFraction(alignment.segmentedChainedFractionB)
        }
        let playedSlots = mergedAndCapped(
            alignment.segmentedSlots
                .filter { $0.aSeconds >= config.minAdSeconds }
                .map { PlayedSlot(
                    startSeconds: $0.aStartSeconds,
                    endSeconds: $0.aEndSeconds,
                    leftRunSeconds: $0.leftFlankSeconds,
                    rightRunSeconds: $0.rightFlankSeconds
                ) },
            config: config
        )
        guard !playedSlots.isEmpty else {
            return .rejectedNonMonotonic(dropped: alignment.runsDroppedNonMonotonic)
        }
        return .accepted(ByteAcceptance(
            chainedFractionB: alignment.segmentedChainedFractionB,
            runsFound: alignment.runsFound,
            runsChained: alignment.segmentedRunsChained,
            playedSlots: playedSlots,
            diagnostics: byteDiagnostics(alignment: alignment, playedSlots: playedSlots)
        ))
    }

    // MARK: - Per-span candidate synthesis (pure)

    /// The per-span candidate + diagnostic + synthesized-slot triple, index-
    /// aligned with `decodedSpans`. Feeds the REUSED `SpliceSlotDispositionEngine`
    /// (via `candidates`) and the REUSED shadow row builder (via `diagnostics`).
    struct CandidateBundle: Sendable, Equatable {
        let candidates: [SpliceSlotCandidate]
        let diagnostics: [SpliceSlotDiagnostics]
        /// The would-be slot per span (`nil` → status-quo width). Same as each
        /// candidate's `.slot`, surfaced for direct assertion.
        let synthesizedSlots: [SpliceSlot?]
    }

    /// Build one candidate per decoded span: pick the best rediff slot OVERLAPPING
    /// the span's presence core (max overlap; ties → earliest start/end), gate it
    /// on `minCoreCoverage`, and synthesize a `SpliceSlot` whose edge stepScores
    /// carry the rediff flank confidence. A span with no qualifying rediff slot
    /// gets `.slot == nil` → status-quo width (rediff-sole-setter contract).
    ///
    /// `coreBankMatch` / `slotBankMatch` are the negative-bank verdict table
    /// (computed in the service; all-false when the bank is dormant), index-
    /// aligned with `decodedSpans`.
    ///
    /// `vetoedRanges` (playhead-xsdz.34 §5): time ranges the user vetoed
    /// (`atomEvidence.filter { .userVetoed }`). Rediff is the SOLE production
    /// width setter and BYPASSES `SpliceSlotResolver`, so it never saw the
    /// resolver's `.vetoNewlyEnclosed` gate. Threading the vetoes here applies
    /// the SAME newly-enclosed rule so a rediff-widened slot cannot absorb a
    /// region the span's core did not already cover. Empty ⇒ status quo.
    static func candidates(
        decodedSpans: [DecodedSpan],
        atomEvidence: [AtomEvidence],
        playedSlots: [PlayedSlot],
        vetoedRanges: [TimeRange] = [],
        coreBankMatch: [Bool],
        slotBankMatch: [Bool],
        config: Configuration = .default
    ) -> CandidateBundle {
        precondition(
            coreBankMatch.count == decodedSpans.count && slotBankMatch.count == decodedSpans.count,
            "bank verdict tables must be index-aligned with decodedSpans"
        )

        var candidates: [SpliceSlotCandidate] = []
        var diagnostics: [SpliceSlotDiagnostics] = []
        var synthesized: [SpliceSlot?] = []
        candidates.reserveCapacity(decodedSpans.count)
        diagnostics.reserveCapacity(decodedSpans.count)
        synthesized.reserveCapacity(decodedSpans.count)

        for (i, span) in decodedSpans.enumerated() {
            let core = TimeRange(start: span.startTime, end: span.endTime)
            let (slot, diag) = resolveSpan(
                core: core, playedSlots: playedSlots, vetoedRanges: vetoedRanges, config: config)
            let intersects = slot.map { s -> Bool in
                let range = TimeRange(start: s.startTime, end: s.endTime)
                // Order-independent existence check — no sort needed.
                return atomEvidence.contains {
                    TimeRange(start: $0.startTime, end: $0.endTime).intersects(range)
                }
            } ?? true
            candidates.append(SpliceSlotCandidate(
                mintedInterval: core,
                slot: slot,
                slotIntersectsAtoms: intersects,
                coreBankMatch: coreBankMatch[i],
                slotBankMatch: slotBankMatch[i]
            ))
            diagnostics.append(diag)
            synthesized.append(slot)
        }
        return CandidateBundle(
            candidates: candidates,
            diagnostics: diagnostics,
            synthesizedSlots: synthesized
        )
    }

    /// Resolve ONE span's core against the played slots. Mirrors the acoustic
    /// resolver's diagnostic contract so the shadow tooling is reused verbatim:
    ///   • degenerate core             → (nil, .degenerateCore, pair=nil)
    ///   • no overlapping rediff slot  → (nil, .noCandidatePairs, pair=nil)
    ///   • overlap but coverage < min  → (nil, .coreCoverageBelowMinimum, pair=slot)
    ///     — UNLESS the FIX A containment clip below applies.
    ///   • slot NEWLY encloses a veto  → (nil, .vetoNewlyEnclosed, pair=slot)
    ///   • qualifying                  → (slot, nil, pair=slot)
    ///
    /// FIX A (playhead-xsdz.58): byte-slot CONTAINMENT CLIP. When Foundation Model
    /// OVER-ANCHORS a span (e.g. conan intro `[0,88.32]` for a true DAI ad of
    /// `[0,60.369]`), the rediff slot holds the byte-exact truth but sits INSIDE
    /// the bloated core, so its `coreCoverage` (`60.369/88.32 = 0.684`) falls below
    /// `minCoreCoverage` and the ordinary gate would DISCARD the correct answer.
    /// This carve-out bypasses the coverage gate — QUALIFYING the slot so the
    /// span CLIPS to the byte-exact extent — but ONLY when the best slot is fully
    /// CONTAINED in the core (`slot.start >= core.start && slot.end <= core.end`)
    /// AND it is the SOLE slot overlapping the core. That double condition makes
    /// the fix safe by construction:
    ///   • A slot that POKES OUT of the core (`start < core.start` OR
    ///     `end > core.end`) is a genuine mis-association → the EXISTING
    ///     `.coreCoverageBelowMinimum` rejection stands, unchanged.
    ///   • A core with NO overlapping slot never reaches here (`.noCandidatePairs`
    ///     above) → baked-in / host-read ad spans the byte differ cannot see keep
    ///     STATUS-QUO width, untouched.
    ///   • A core overlapping 2+ slots (whether contained or poking out) does NOT
    ///     clip — clipping to one slot could DROP a byte-confirmed ad, and the
    ///     one-slot-per-span downstream machinery (`candidates` → disposition →
    ///     rewrite is index-aligned per decoded span) cannot split a span without
    ///     restructuring. So a multi-slot core falls to STATUS-QUO (keeps its full
    ///     minted width, which still ENCLOSES every contained slot → nothing
    ///     dropped). See the multi-slot test / xsdz.58 report for the rationale.
    /// Mid-rolls are UNAFFECTED — they qualify via the ordinary `coverage >= min`
    /// branch and never reach this carve-out.
    static func resolveSpan(
        core: TimeRange,
        playedSlots: [PlayedSlot],
        vetoedRanges: [TimeRange] = [],
        config: Configuration = .default
    ) -> (slot: SpliceSlot?, diagnostics: SpliceSlotDiagnostics) {
        guard core.length > 0 else {
            return (nil, SpliceSlotDiagnostics(bestGeometryValidPair: nil, failureReason: .degenerateCore))
        }
        // Best OVERLAPPING slot: max positive overlap, ties → earliest start/end.
        // `overlappingCount` gates the FIX A clip to the unambiguous single-slot
        // case (a 2+-slot core must NOT clip to one and drop the others).
        var best: (slot: PlayedSlot, overlap: Double)?
        var overlappingCount = 0
        for played in playedSlots {
            let range = TimeRange(start: played.startSeconds, end: played.endSeconds)
            let overlap = core.overlapLength(with: range)
            guard overlap > 0 else { continue }
            overlappingCount += 1
            if let current = best {
                if overlap > current.overlap
                    || (overlap == current.overlap && played.startSeconds < current.slot.startSeconds)
                    || (overlap == current.overlap && played.startSeconds == current.slot.startSeconds
                        && played.endSeconds < current.slot.endSeconds) {
                    best = (played, overlap)
                }
            } else {
                best = (played, overlap)
            }
        }
        guard let match = best else {
            return (nil, SpliceSlotDiagnostics(bestGeometryValidPair: nil, failureReason: .noCandidatePairs))
        }
        let slot = synthesizeSlot(from: match.slot, core: core, overlap: match.overlap)
        if slot.coreCoverage < config.minCoreCoverage {
            // FIX A (playhead-xsdz.58): bypass the coverage gate ONLY for a single
            // byte-exact slot fully contained in an over-anchored core; otherwise
            // keep the existing `.coreCoverageBelowMinimum` rejection verbatim.
            let containedInCore =
                match.slot.startSeconds >= core.start && match.slot.endSeconds <= core.end
            let soleOverlappingSlot = overlappingCount == 1
            guard containedInCore && soleOverlappingSlot else {
                return (nil, SpliceSlotDiagnostics(bestGeometryValidPair: slot, failureReason: .coreCoverageBelowMinimum))
            }
        }
        // playhead-xsdz.34 §5: the SAME newly-enclosed rule `SpliceSlotResolver`
        // applies (`SpliceSlotResolver.swift`, `.vetoNewlyEnclosed`). If the
        // synthesized slot would NEWLY enclose a vetoed range the span's core
        // does NOT already intersect, reject the widening → status-quo width (no
        // absorption). A veto INSIDE the core (core already intersects it) does
        // not fire — the slot is not newly enclosing anything.
        let slotRange = TimeRange(start: slot.startTime, end: slot.endTime)
        for veto in vetoedRanges where slotRange.intersects(veto) && !core.intersects(veto) {
            return (nil, SpliceSlotDiagnostics(bestGeometryValidPair: slot, failureReason: .vetoNewlyEnclosed))
        }
        return (slot, SpliceSlotDiagnostics(bestGeometryValidPair: slot, failureReason: nil))
    }

    /// Synthesize a `SpliceSlot` from a played slot + the core it covers. Edge
    /// stepScores carry the rediff flank confidence (`1 - exp(-flankSeconds/60)`,
    /// the differ's own curve); `slotConfidence` is the weaker edge; `coreCoverage`
    /// is `|slot ∩ core| / |core|`.
    static func synthesizeSlot(from played: PlayedSlot, core: TimeRange, overlap: Double) -> SpliceSlot {
        let startScore = flankConfidence(played.leftRunSeconds)
        let endScore = flankConfidence(played.rightRunSeconds)
        return SpliceSlot(
            startTime: played.startSeconds,
            endTime: played.endSeconds,
            startEdge: SpliceEdgeEvidence(time: played.startSeconds, stepScore: startScore, contributingSignals: 0),
            endEdge: SpliceEdgeEvidence(time: played.endSeconds, stepScore: endScore, contributingSignals: 0),
            slotConfidence: min(startScore, endScore),
            coreCoverage: core.length > 0 ? overlap / core.length : 0
        )
    }

    /// The differ's flank-confidence curve for ONE side: `1 - exp(-seconds/60)`.
    /// Reuses `RediffDiffer.confidence` (min of two equal flanks = that flank) so
    /// the curve has a single source of truth.
    static func flankConfidence(_ seconds: Double) -> Double {
        RediffDiffer.confidence(leftRunSeconds: seconds, rightRunSeconds: seconds)
    }
}

// MARK: - B-side provider seam (STUB — device capture / live re-fetch DEFERRED)

/// Supplies the re-fetched B-side audio for a rediff comparison.
///
/// DEFERRED (per xsdz.29 size guidance): the two SOURCES that would populate this
/// in production are follow-up beads, not this one —
///   1. the iOS 27 `AVPlayerItemSampleBufferOutput` AS-PLAYED tap (design input
///      1): fingerprints the literally-played, DAI-stitched bytes incrementally
///      (new API territory needing a device); and
///   2. the WiFi+charging re-fetch scheduler + Strategy-C pre-check (xsdz.28
///      policy).
/// This bead delivers the OFFLINE oracle: it takes the B-side as an INPUT. In
/// production no provider is injected (`nil`) so the rediff pass no-ops and the
/// pipeline is byte-identical; tests inject a provider directly.
///
/// The provider returns RAW mono 16 kHz PCM (the analysis pipeline's decode
/// rate), NEVER fingerprints — `RediffSlotOwnership.gateAndDiff` applies the
/// EXACT xsdz.27 resample→fingerprint extractor to both A and B (design input 2).
protocol RediffBSideProvider: Sendable {
    /// The re-fetched B-side copy as mono 16 kHz PCM for `assetId`, or `nil` when
    /// no re-fetch is available (the common production case today → no-op).
    func refetchedBSideMono16kHz(assetId: String) async -> [Float]?

    /// playhead-xsdz.57: the re-fetched B-side copy as its RAW on-disk file for
    /// `assetId`, or `nil` when no byte-level re-fetch is available. Feeds the
    /// byte-run aligner — the PRIMARY differ — which needs the container bytes,
    /// not PCM. Defaulted to `nil` (see the extension below) so chroma-only
    /// providers compile unchanged and simply never engage the byte path.
    /// The service anchors the returned URL (regular, unaliased, non-empty
    /// file — bf4a2383 precedent) before reading a byte, and the bytes NEVER
    /// outlive the diff (xsdz.28 never-persist-B).
    func refetchedBSideFileURL(assetId: String) async -> URL?

    /// playhead-xsdz.36.2 (k-way): ALL staged B-side files for `assetId` — the K
    /// distinct-persona re-fetches the byte differ aligns A against and unions.
    ///
    /// A PROTOCOL REQUIREMENT (not extension-only) so a concrete provider's
    /// override is DYNAMICALLY dispatched through the `any RediffBSideProvider`
    /// existential the service holds — an extension-only method would statically
    /// bind to the default and silently drop every B-side but the first. The
    /// default (see below) wraps the single `refetchedBSideFileURL` so pre-k-way
    /// providers drive exactly today's one alignment.
    func refetchedBSideFileURLs(assetId: String) async -> [URL]
}

extension RediffBSideProvider {
    /// Default: no byte-level B-side — the byte path falls back to chroma.
    func refetchedBSideFileURL(assetId: String) async -> URL? { nil }

    /// Default: the single `refetchedBSideFileURL` as a one-element list (or
    /// empty) — one alignment, exactly the pre-k-way behavior.
    func refetchedBSideFileURLs(assetId: String) async -> [URL] {
        if let url = await refetchedBSideFileURL(assetId: assetId) { return [url] }
        return []
    }
}

// MARK: - Rediff shadow breadcrumb (distinct tag; reuses the frozen row type)

/// A rediff-sourced sibling of `SpliceSlotShadowBreadcrumb`. Emits the IDENTICAL
/// frozen `SpliceSlotShadowRow` field set under a DISTINCT `rediffslot.shadow`
/// tag so the dogfood log + activation eval can attribute a shadow row to the
/// rediff oracle vs the acoustic splice channel WITHOUT modifying the frozen v3
/// `spliceslot.shadow` formatter. The structured rows record to a separate
/// `SpliceSlotShadowObserver` instance in the service (rediff rows never
/// comingle with acoustic rows).
enum RediffSlotShadowBreadcrumb {
    static let subsystem = "com.playhead"

    static func format(_ row: SpliceSlotShadowRow) -> String {
        "rediffslot.shadow"
            + " assetId=\(row.assetId)"
            + " mintedStart=\(SpliceSlotShadowBreadcrumb.fmt(row.mintedStart))"
            + " mintedEnd=\(SpliceSlotShadowBreadcrumb.fmt(row.mintedEnd))"
            + " slotStart=\(SpliceSlotShadowBreadcrumb.fmt(row.slotStart))"
            + " slotEnd=\(SpliceSlotShadowBreadcrumb.fmt(row.slotEnd))"
            + " widthDeltaSec=\(SpliceSlotShadowBreadcrumb.fmt(row.widthDeltaSec))"
            + " startEdgeScore=\(SpliceSlotShadowBreadcrumb.fmt(row.startEdgeScore))"
            + " endEdgeScore=\(SpliceSlotShadowBreadcrumb.fmt(row.endEdgeScore))"
            + " coreCoverage=\(SpliceSlotShadowBreadcrumb.fmt(row.coreCoverage))"
            + " qualified=\(row.qualified)"
            + " reason=\(row.reason.rawValue)"
    }
}
