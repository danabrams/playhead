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
        guard !slotsA.isEmpty else { return [] }
        // Sort by start (differ output is already ordered, but be defensive).
        let sorted = slotsA.sorted { ($0.startSeconds, $0.endSeconds) < ($1.startSeconds, $1.endSeconds) }
        var merged: [PlayedSlot] = [PlayedSlot(
            startSeconds: sorted[0].startSeconds,
            endSeconds: sorted[0].endSeconds,
            leftRunSeconds: sorted[0].leftRunSeconds,
            rightRunSeconds: sorted[0].rightRunSeconds
        )]
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
    ///   • slot NEWLY encloses a veto  → (nil, .vetoNewlyEnclosed, pair=slot)
    ///   • qualifying                  → (slot, nil, pair=slot)
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
        var best: (slot: PlayedSlot, overlap: Double)?
        for played in playedSlots {
            let range = TimeRange(start: played.startSeconds, end: played.endSeconds)
            let overlap = core.overlapLength(with: range)
            guard overlap > 0 else { continue }
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
        guard slot.coreCoverage >= config.minCoreCoverage else {
            return (nil, SpliceSlotDiagnostics(bestGeometryValidPair: slot, failureReason: .coreCoverageBelowMinimum))
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
